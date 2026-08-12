// FundPicker — иерархический выбор фондов для /fund-trades: список ФОНДОВ,
// СГРУППИРОВАННЫХ ПО УК. Заголовок-группа = аватар УК (UK_LOGOS) + имя УК;
// под ним фонды-строки: чекбокс(multi)/радио(single) + тикер + короткое имя.
//
// Триггер-кнопка ОТКРЫВАЕТ МОДАЛЬНОЕ ОКНО (паттерн AssetPickerModal): overlay
// rgba(0,0,0,.6) клик=закрыть, окно bg-secondary + 2px border var(--text-primary)
// + hard-shadow, maxWidth ~580, max-h 80vh, скролл, заголовок + X. Внутри окна —
// поиск (по тикеру/имени), УК-группы и фонды-строки с hover-подсветкой.
//
// mode='multi'  (Потоки):   чекбоксы, множественный выбор; «Все фонды» сверху,
//                           выбор НЕ закрывает окно (закрытие — «Готово»/X/overlay).
//                           Кнопка-триггер = «N фондов» / «Все фонды».
// mode='single' (Снапшот):  радио, ровно один фонд; выбор ЗАКРЫВАЕТ окно.
//                           Кнопка-триггер = «тикер · имя» с аватаром УК.

import { useEffect, useMemo, useRef, useState } from 'react';
import ModalLayer from '../ModalLayer';
import { MODAL_LAYER_Z } from '../../utils/modalHost';
import { Search, X } from 'lucide-react';
import { UK_LOGOS, stripUkName } from '../../config/fundConfig';
import { useViewportWidth } from '../../hooks/useViewportWidth';
import HelpTooltip from '../HelpTooltip';
import { INDEX_FUNDS_HELP } from './PortfolioFundPicker';

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
    /** Тикеры индексных фондов. Заданы (multi) → в шапке модалки появляется
     *  таблетка-тумблер «Без индексных фондов» (зеркало PortfolioFundPicker):
     *  прожата ⇔ ни один индексный тикер не отмечен. */
    indexTickers?: string[];
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

// ── Модальное окно выбора фондов ───────────────────────────────────────────────
// Структура зеркалит AssetPickerModal (overlay + окно с поиском и списком).
// Управление выбором через те же selected/onChange; закрытие — onClose.
function FundPickerModal({
    funds,
    mode,
    selected,
    onChange,
    onClose,
    indexTickers = [],
}: {
    funds: FundPickerFund[];
    mode: 'multi' | 'single';
    selected: Set<string>;
    onChange: (next: Set<string>) => void;
    onClose: () => void;
    indexTickers?: string[];
}) {
    const [searchQuery, setSearchQuery] = useState('');
    const [hover, setHover] = useState<string | null>(null);
    // На мобиле делаем ✕ заметной кнопкой с обводкой (как панель УК) — бледный
    // text-secondary X терялся. Десктоп — прежний минималистичный X.
    const isMobile = useViewportWidth() < 768;

    // Autofocus поиска — только на desktop (mouse). На мобиле не дёргаем
    // клавиатуру, чтобы пользователь сначала увидел список (как AssetPickerModal).
    const inputRef = useRef<HTMLInputElement>(null);
    useEffect(() => {
        // Надёжная детекция тача: '(hover: none)' одна ненадёжна (гибриды/
        // desktop-mode рапортуют hover) → добавляем pointer:coarse и maxTouchPoints.
        const isTouch = window.matchMedia('(hover: none), (pointer: coarse)').matches
            || (navigator.maxTouchPoints ?? 0) > 0;
        if (!isTouch && inputRef.current) inputRef.current.focus();
    }, []);

    // Esc закрывает окно.
    useEffect(() => {
        const onKey = (e: KeyboardEvent) => {
            if (e.key === 'Escape') onClose();
        };
        window.addEventListener('keydown', onKey);
        return () => window.removeEventListener('keydown', onKey);
    }, [onClose]);

    const allActive = mode === 'multi' && selected.size === 0;

    // Тумблер «Без индексных фондов» — не отдельное состояние, а срез выбора:
    // прожат ⇔ ни один индексный фонд не отмечен (зеркало PortfolioFundPicker).
    // «Пусто = все фонды», поэтому пустой набор разворачиваем в полный: иначе
    // индексные считались бы снятыми там, где на деле выбраны все.
    const allTickers = useMemo(() => funds.map((f) => f.ticker), [funds]);
    const idxTickers = useMemo(
        () => indexTickers.filter((t) => allTickers.includes(t)),
        [indexTickers, allTickers],
    );
    const effective = useMemo(
        () => (selected.size > 0 ? selected : new Set(allTickers)),
        [selected, allTickers],
    );
    const indexOff = mode === 'multi' && idxTickers.length > 0
        && idxTickers.every((t) => !effective.has(t));
    const toggleIndexOff = () => {
        const next = new Set(effective);
        if (indexOff) idxTickers.forEach((t) => next.add(t));
        else idxTickers.forEach((t) => next.delete(t));
        // Полный набор канонически схлопываем в пусто = «все фонды».
        const covered = allTickers.every((t) => next.has(t));
        onChange(covered ? new Set() : next);
    };

    // Фильтр по тикеру/имени (нечувствителен к регистру). Группировка — после
    // фильтрации, чтобы пустые УК не показывались.
    const q = searchQuery.trim().toLowerCase();
    const filtered = q
        ? funds.filter(
            (f) =>
                f.ticker.toLowerCase().includes(q) ||
                f.name.toLowerCase().includes(q),
        )
        : funds;
    const groups = useMemo(() => groupByUk(filtered), [filtered]);

    const toggleMulti = (ticker: string) => {
        const next = new Set(selected);
        if (next.has(ticker)) next.delete(ticker);
        else next.add(ticker);
        onChange(next);
    };

    // Выбрать/снять ВЕСЬ УК (все его фонды) — чекбокс на заголовке группы (multi).
    const toggleGroup = (g: UkGroup) => {
        const tickers = g.funds.map((f) => f.ticker);
        const allOn = tickers.every((t) => selected.has(t));
        const next = new Set(selected);
        if (allOn) tickers.forEach((t) => next.delete(t));
        else tickers.forEach((t) => next.add(t));
        onChange(next);
    };

    const pickSingle = (ticker: string) => {
        onChange(new Set([ticker]));
        onClose();
    };

    return (
        <ModalLayer>
            // role="dialog" — драг-обработчик панели песочницы игнорирует клики
            // внутри [role="dialog"] (см. onDragStart в SandboxPage).
            <div role="dialog" aria-modal="true" className="instrument-modal-root fixed inset-0 z-50 flex items-start justify-center p-4 pt-20" style={{ zIndex: MODAL_LAYER_Z }}>
                {/* Backdrop — solid dim без backdrop-blur (editorial: no glass effects). */}
                <div
                    className="absolute inset-0"
                    style={{ backgroundColor: 'rgba(0,0,0,0.6)' }}
                    onClick={onClose}
                />

                {/* Окно — bg-secondary + 2px border text-primary + hard shadow. */}
                <div
                    className="instrument-modal relative w-full rounded-2xl max-h-[80vh] overflow-hidden flex flex-col"
                    style={{
                        maxWidth: 580,
                        backgroundColor: 'var(--bg-secondary)',
                        border: '2px solid var(--text-primary)',
                        boxShadow: 'var(--shadow-hard-chip, 6px 6px 0 var(--text-primary))',
                        color: 'var(--text-primary)',
                    }}
                >
                    {/* Header */}
                    <div className="px-6 pt-6 pb-4">
                        <div className="flex items-center justify-between mb-6">
                            <h2 className="text-2xl font-bold" style={{ color: 'var(--text-primary)' }}>
                                Выбор фонда
                            </h2>
                            <button
                                onClick={onClose}
                                className="instrument-modal-close transition-colors"
                                style={isMobile ? {
                                    display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
                                    width: 44, height: 44, flexShrink: 0, borderRadius: 8,
                                    border: '1.5px solid var(--text-primary)',
                                    background: 'var(--bg-primary)', color: 'var(--text-primary)',
                                } : { color: 'var(--text-secondary)', padding: 8, borderRadius: 8 }}
                                aria-label="Закрыть"
                            >
                                <X size={isMobile ? 18 : 24} strokeWidth={isMobile ? 2.4 : 2} />
                            </button>
                        </div>

                        {/* Search */}
                        <div className="relative">
                            <Search
                                size={20}
                                className="absolute left-4 top-1/2 -translate-y-1/2"
                                style={{ color: 'var(--text-secondary)' }}
                            />
                            <input
                                ref={inputRef}
                                type="text"
                                value={searchQuery}
                                onChange={(e) => setSearchQuery(e.target.value)}
                                placeholder="Поиск по тикеру или имени"
                                className="instrument-modal-search w-full pl-12 pr-4 py-4 text-base rounded-xl focus:outline-none transition-colors"
                                style={{
                                    backgroundColor: 'var(--bg-primary)',
                                    color: 'var(--text-primary)',
                                    border: '2px solid var(--text-primary)',
                                }}
                            />
                        </div>

                        {/* Таблетка-тумблер «Без индексных фондов» под поиском: прожата —
                            индексные фонды сняты с выбора, рядом «?» с объяснением. */}
                        {mode === 'multi' && idxTickers.length > 0 && (
                            <div className="flex items-center mt-4" style={{ gap: 6 }}>
                                <button
                                    type="button"
                                    onClick={toggleIndexOff}
                                    className="editorial-press"
                                    aria-pressed={indexOff}
                                    title={indexOff
                                        ? `Индексные фонды выключены (${idxTickers.length})`
                                        : `Выключить индексные фонды (${idxTickers.length})`}
                                    style={{
                                        padding: '4px 14px',
                                        borderRadius: 999,
                                        border: '2px solid var(--text-primary)',
                                        background: indexOff ? 'var(--accent)' : 'var(--bg-secondary)',
                                        color: indexOff ? 'var(--text-inverse)' : 'var(--text-primary)',
                                        fontSize: 'var(--fs-xs)',
                                        fontWeight: 700,
                                        cursor: 'pointer',
                                        whiteSpace: 'nowrap',
                                        boxShadow: indexOff ? '3px 3px 0 var(--text-primary)' : 'none',
                                        transition: 'background-color 0.12s ease, color 0.12s ease',
                                    }}
                                >
                                    Без индексных фондов
                                </button>
                                {/* float — окно с overflow:hidden обрезало бы поповер. */}
                                <HelpTooltip content={INDEX_FUNDS_HELP} size={16} float />
                            </div>
                        )}
                    </div>

                    {/* Results */}
                    <div className="overflow-y-auto flex-1 px-6 styled-scrollbar">
                        {/* «Все фонды» — только в multi, сброс selected в пусто. Не закрывает окно. */}
                        {mode === 'multi' && !q && (
                            <button
                                type="button"
                                onClick={() => onChange(new Set())}
                                onMouseEnter={() => setHover('__all__')}
                                onMouseLeave={() => setHover(null)}
                                style={{
                                    display: 'flex',
                                    alignItems: 'center',
                                    gap: 10,
                                    width: '100%',
                                    padding: '12px 14px',
                                    textAlign: 'left',
                                    cursor: 'pointer',
                                    background: allActive
                                        ? 'var(--bg-primary)'
                                        : (hover === '__all__' ? 'color-mix(in srgb, var(--accent) 10%, transparent)' : 'transparent'),
                                    color: 'var(--text-primary)',
                                    border: 'none',
                                    borderBottom: '1px solid color-mix(in srgb, var(--text-primary) 12%, transparent)',
                                    fontSize: 'var(--fs-sm)',
                                    fontWeight: allActive ? 800 : 600,
                                    whiteSpace: 'nowrap',
                                    borderRadius: 8,
                                }}
                            >
                                <span
                                    style={{
                                        width: 18,
                                        height: 18,
                                        flexShrink: 0,
                                        display: 'flex',
                                        alignItems: 'center',
                                        justifyContent: 'center',
                                        borderRadius: 4,
                                        background: allActive ? 'var(--accent)' : 'transparent',
                                        border: `2px solid ${allActive ? 'var(--accent)' : 'var(--text-muted)'}`,
                                        transition: 'all 150ms',
                                    }}
                                >
                                    {allActive && (
                                        <svg viewBox="0 0 14 14" width="11" height="11" fill="none" stroke="var(--text-inverse)" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                                            <polyline points="2 7 6 11 12 3" />
                                        </svg>
                                    )}
                                </span>
                                Все фонды
                            </button>
                        )}

                        {groups.length === 0 ? (
                            <div className="py-12 text-center" style={{ color: 'var(--text-secondary)' }}>
                                {funds.length === 0 ? 'Нет фондов' : 'Ничего не найдено'}
                            </div>
                        ) : (
                            groups.map((g) => (
                                <div key={g.key}>
                                    {/* Заголовок-группа УК. В multi — кликабелен: чекбокс выбирает ВЕСЬ УК. */}
                                    {(() => {
                                        const gTickers = g.funds.map((f) => f.ticker);
                                        const gOn = gTickers.filter((t) => selected.has(t)).length;
                                        const gAll = gOn === gTickers.length && gTickers.length > 0;
                                        const gHover = hover === `__grp__${g.key}`;
                                        return (
                                            <div
                                                onClick={mode === 'multi' ? () => toggleGroup(g) : undefined}
                                                onMouseEnter={mode === 'multi' ? () => setHover(`__grp__${g.key}`) : undefined}
                                                onMouseLeave={mode === 'multi' ? () => setHover(null) : undefined}
                                                style={{
                                                    display: 'flex',
                                                    alignItems: 'center',
                                                    gap: 8,
                                                    padding: '10px 14px 6px',
                                                    position: 'sticky',
                                                    top: 0,
                                                    zIndex: 1,
                                                    background: mode === 'multi' && gHover
                                                        ? 'color-mix(in srgb, var(--accent) 8%, var(--bg-secondary))'
                                                        : 'var(--bg-secondary)',
                                                    borderBottom: '1px solid color-mix(in srgb, var(--text-primary) 12%, transparent)',
                                                    cursor: mode === 'multi' ? 'pointer' : 'default',
                                                    transition: 'background 150ms',
                                                }}
                                            >
                                                {mode === 'multi' && (
                                                    <span
                                                        style={{
                                                            width: 18,
                                                            height: 18,
                                                            flexShrink: 0,
                                                            display: 'flex',
                                                            alignItems: 'center',
                                                            justifyContent: 'center',
                                                            borderRadius: 4,
                                                            background: gAll ? 'var(--accent)' : 'transparent',
                                                            border: `2px solid ${gAll || gOn > 0 ? 'var(--accent)' : 'var(--text-muted)'}`,
                                                            transition: 'all 150ms',
                                                        }}
                                                    >
                                                        {gAll ? (
                                                            <svg viewBox="0 0 14 14" width="11" height="11" fill="none" stroke="var(--text-inverse)" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                                                                <polyline points="2 7 6 11 12 3" />
                                                            </svg>
                                                        ) : gOn > 0 ? (
                                                            <span style={{ width: 8, height: 2, background: 'var(--accent)', borderRadius: 1 }} />
                                                        ) : null}
                                                    </span>
                                                )}
                                                <UkAvatar ukId={g.ukId} ukName={g.name} size={22} />
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
                                                        flex: 1,
                                                        minWidth: 0,
                                                    }}
                                                >
                                                    {g.name}
                                                </span>
                                                {mode === 'multi' && gOn > 0 && (
                                                    <span style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-muted)', flexShrink: 0, fontWeight: 700 }}>
                                                        {gOn}/{gTickers.length}
                                                    </span>
                                                )}
                                            </div>
                                        );
                                    })()}

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
                                                    gap: 12,
                                                    width: '100%',
                                                    padding: '10px 14px',
                                                    textAlign: 'left',
                                                    cursor: 'pointer',
                                                    background: on
                                                        ? 'var(--bg-primary)'
                                                        : (isHover ? 'color-mix(in srgb, var(--accent) 10%, transparent)' : 'transparent'),
                                                    color: 'var(--text-primary)',
                                                    border: 'none',
                                                    fontSize: 'var(--fs-sm)',
                                                    fontWeight: on ? 700 : 500,
                                                    whiteSpace: 'nowrap',
                                                    borderRadius: 8,
                                                }}
                                            >
                                                {/* Индикатор выбора: квадрат-чекбокс (multi) / круг-радио (single) */}
                                                <span
                                                    style={{
                                                        width: 18,
                                                        height: 18,
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
                                                        ? <span style={{ width: 7, height: 7, borderRadius: '50%', background: 'var(--text-inverse)' }} />
                                                        : (
                                                            <svg viewBox="0 0 14 14" width="11" height="11" fill="none" stroke="var(--text-inverse)" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                                                                <polyline points="2 7 6 11 12 3" />
                                                            </svg>
                                                        ))}
                                                </span>
                                                <span
                                                    style={{
                                                        fontWeight: 700,
                                                        flex: 1,
                                                        minWidth: 0,
                                                        overflow: 'hidden',
                                                        textOverflow: 'ellipsis',
                                                        color: 'var(--text-primary)',
                                                    }}
                                                    title={f.name}
                                                >
                                                    {stripUkName(f.name, g.ukId)}
                                                </span>
                                                <span
                                                    style={{
                                                        fontFamily: 'var(--font-mono, monospace)',
                                                        fontSize: 'var(--fs-xs)',
                                                        flexShrink: 0,
                                                        color: 'var(--text-secondary)',
                                                    }}
                                                >
                                                    {f.ticker}
                                                </span>
                                            </button>
                                        );
                                    })}
                                </div>
                            ))
                        )}
                    </div>

                    {/* Footer — только multi: «Готово» закрывает окно (выбор уже применён). */}
                    {mode === 'multi' && (
                        <div
                            className="px-6 py-4"
                            style={{ borderTop: '1px solid color-mix(in srgb, var(--text-primary) 12%, transparent)' }}
                        >
                            <button
                                type="button"
                                onClick={onClose}
                                className="editorial-press"
                                style={{
                                    width: '100%',
                                    padding: '12px 18px',
                                    background: 'var(--accent)',
                                    color: 'var(--text-inverse)',
                                    border: '2px solid var(--text-primary)',
                                    borderRadius: 12,
                                    fontSize: 'var(--fs-sm)',
                                    fontWeight: 700,
                                    cursor: 'pointer',
                                    boxShadow: '3px 3px 0 var(--text-primary)',
                                }}
                            >
                                Готово{allActive ? '' : ` · ${selected.size}`}
                            </button>
                        </div>
                    )}
                </div>
            </div>
        </ModalLayer>
    );
}

export default function FundPicker({
    funds,
    mode,
    selected,
    onChange,
    buttonLabel,
    minWidth = 220,
    indexTickers,
}: FundPickerProps) {
    const [open, setOpen] = useState(false);

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

    return (
        <div style={{ display: 'inline-flex', minWidth: 0 }}>
            <button
                type="button"
                onClick={() => setOpen(true)}
                className="editorial-press"
                style={{
                    display: 'inline-flex',
                    alignItems: 'center',
                    gap: 6,
                    padding: '8px 18px',
                    minWidth: mode === 'single' ? minWidth : undefined,
                    maxWidth: 320,
                    background: active ? 'var(--accent)' : 'var(--bg-secondary)',
                    color: active ? 'var(--text-inverse)' : 'var(--text-primary)',
                    border: '2px solid var(--text-primary)',
                    borderRadius: 999,
                    fontSize: 'var(--fs-sm)',
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
                <FundPickerModal
                    funds={funds}
                    mode={mode}
                    selected={selected}
                    onChange={onChange}
                    onClose={() => setOpen(false)}
                    indexTickers={indexTickers}
                />
            )}
        </div>
    );
}
