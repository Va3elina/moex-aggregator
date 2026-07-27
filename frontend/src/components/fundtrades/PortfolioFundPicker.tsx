// PortfolioFundPicker — выбор конкретных фондов для «Общего портфеля».
// Дизайн-код заимствован у виджета выбора фондов в «Деньгах в фондах»
// (FundsMoneyPage fundPickerOpen-модалка + FundsTable bare): top-anchored окно
// maxWidth 816 с editorial-рамкой и hard-shadow, внутри таблица с фиксированной
// раскладкой колонок — чекбокс · Название (лого УК + имя + тикер) · СЧА ·
// Доходность, сортировка по СЧА/доходности, строка «Выбрать все» и групповые
// чекбоксы. Отличия от «Денег в фондах»: группировка по УК (не подкатегориям),
// семантика «выбрано» (не «скрыто»), и выбор применяется по закрытию окна.
//
// Черновик выбора живёт внутри модалки и применяется на «Готово»/X/overlay —
// промежуточное состояние «сняты все» не улетает в API (пусто = все фонды).
// selected: Set тикеров, пусто = все (канон: полный набор схлопывается в пусто).

import React, { useMemo, useState } from 'react';
import { X, Check, Minus, ChevronDown, ChevronUp, ChevronsUpDown } from 'lucide-react';
import { resolveFundLogo, stripUkName, ukAbbr, UK_LOGOS } from '../../config/fundConfig';
import type { FundWithHistory } from '../../services/api';

const SOFT_BORDER = '1px solid color-mix(in srgb, var(--text-primary) 12%, transparent)';
const NAME_FADE = 'linear-gradient(to right, #000 calc(100% - 30px), transparent 100%)';

// Числа СЧА/доходности — как в bare-FundsTable (OI_NUM_STYLE): 600, fs-sm,
// приглушённый серый, табличные цифры. Цвет доходности перекрывается семантикой.
const NUM_STYLE: React.CSSProperties = {
    fontWeight: 600,
    fontSize: 'var(--fs-sm)',
    color: 'var(--text-secondary)',
    fontVariantNumeric: 'tabular-nums',
};

// Типографика заголовков колонок — шапка поиска ОИ (OI_HEAD_STYLE в FundsTable).
const HEAD_STYLE: React.CSSProperties = {
    fontWeight: 800,
    textTransform: 'uppercase',
    letterSpacing: '0.04em',
    color: 'var(--text-secondary)',
};

// Чекбокс — акцентный скруглённый квадрат с галочкой/минусом (CheckBox из
// bare-FundsTable, один в один).
function CheckBox({ checked, indeterminate }: { checked: boolean; indeterminate?: boolean }) {
    const on = checked || indeterminate;
    return (
        <span
            style={{
                width: 18,
                height: 18,
                flexShrink: 0,
                borderRadius: 5,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                background: on ? 'var(--accent)' : 'transparent',
                border: on
                    ? '1.5px solid var(--accent)'
                    : '1.5px solid color-mix(in srgb, var(--text-primary) 32%, transparent)',
                color: 'var(--text-inverse)',
            }}
            aria-hidden="true"
        >
            {indeterminate ? <Minus size={12} strokeWidth={3.5} /> : checked ? <Check size={12} strokeWidth={3.5} /> : null}
        </span>
    );
}

// Fade-out длинных имён вместо троеточия (FadedName из FundsTable): маска
// накладывается только когда текст реально обрезан.
function FadedName({ name, display }: { name: string; display: string }) {
    const ref = React.useRef<HTMLSpanElement>(null);
    const [clipped, setClipped] = React.useState(false);
    React.useLayoutEffect(() => {
        const el = ref.current;
        if (!el) return;
        const check = () => setClipped(el.scrollWidth > el.clientWidth + 1);
        check();
        const ro = new ResizeObserver(check);
        ro.observe(el);
        return () => ro.disconnect();
    }, [display]);
    return (
        <span
            ref={ref}
            title={name}
            className="font-bold"
            style={{
                fontSize: 'var(--fs-sm)',
                flex: '0 1 auto',
                minWidth: 0,
                overflow: 'hidden',
                whiteSpace: 'nowrap',
                ...(clipped ? { maskImage: NAME_FADE, WebkitMaskImage: NAME_FADE } : {}),
            }}
        >
            {display}
        </span>
    );
}

// Лучшая доступная доходность (bestReturn из FundsTable): 1г → 6м → 3м → 1м,
// молодые фонды показывают короткий период с подписью вместо «—».
function bestReturn(r?: FundWithHistory['returns']): { v: number; label: string } | null {
    if (!r) return null;
    if (r.y1 != null) return { v: r.y1, label: '1г' };
    if (r.m6 != null) return { v: r.m6, label: '6м' };
    if (r.m3 != null) return { v: r.m3, label: '3м' };
    if (r.m1 != null) return { v: r.m1, label: '1м' };
    return null;
}

interface UkGroup {
    key: string;
    ukId?: number | string | null;
    name: string;
    funds: FundWithHistory[];
}

// Группировка по УК: ключ — uk_id (приоритет), иначе имя; имя — канон из UK_LOGOS.
function groupByUk(funds: FundWithHistory[]): UkGroup[] {
    const order: string[] = [];
    const map = new Map<string, UkGroup>();
    for (const f of funds) {
        const key = f.uk_id != null && f.uk_id !== '' ? `id:${f.uk_id}` : `nm:${(f.uk ?? '').trim() || '—'}`;
        let g = map.get(key);
        if (!g) {
            const logo = f.uk_id != null ? UK_LOGOS[String(f.uk_id)] : undefined;
            g = { key, ukId: f.uk_id, name: logo?.name ?? (f.uk ?? '').trim() ?? 'Прочие', funds: [] };
            map.set(key, g);
            order.push(key);
        }
        g.funds.push(f);
    }
    return order.map((k) => map.get(k)!);
}

// ── Модалка (шелл — fundPickerOpen из FundsMoneyPage) ────────────────────────
function PickerModal({
    funds,
    selected,
    onApply,
    onClose,
}: {
    funds: FundWithHistory[];
    selected: Set<string>;
    onApply: (next: Set<string>) => void;
    onClose: () => void;
}) {
    const allTickers = useMemo(() => funds.map((f) => f.ticker), [funds]);
    // Черновик: пусто в persisted-наборе = все выбраны.
    const [draft, setDraft] = useState<Set<string>>(
        () => (selected.size === 0 ? new Set(allTickers) : new Set(selected)),
    );
    const [collapsed, setCollapsed] = useState<Set<string>>(new Set());
    const [sortKey, setSortKey] = useState<'nav' | 'y1'>('nav');
    const [sortDir, setSortDir] = useState<'desc' | 'asc'>('desc');

    // Применение при любом закрытии: полный набор (или пустой черновик, который
    // для API неотличим от «все») схлопываем в пусто = «все фонды».
    const applyAndClose = () => {
        const covered = draft.size === 0 || allTickers.every((t) => draft.has(t));
        onApply(covered ? new Set() : new Set(draft));
        onClose();
    };

    // Esc закрывает с применением (как overlay/X).
    React.useEffect(() => {
        const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') applyAndClose(); };
        window.addEventListener('keydown', onKey);
        return () => window.removeEventListener('keydown', onKey);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [draft]);

    const toggleSort = (key: 'nav' | 'y1') => {
        if (sortKey === key) setSortDir((d) => (d === 'desc' ? 'asc' : 'desc'));
        else { setSortKey(key); setSortDir('desc'); }
    };

    // Заголовок сортируемой колонки — renderSortButton из bare-FundsTable.
    const sortButton = (col: 'nav' | 'y1', label: string, title?: string) => {
        const active = sortKey === col;
        return (
            <button
                type="button"
                onClick={() => toggleSort(col)}
                title={title}
                className="inline-flex items-center justify-end uppercase whitespace-nowrap transition-colors"
                onMouseEnter={(e) => {
                    e.currentTarget.style.background = 'color-mix(in srgb, var(--text-primary) 8%, transparent)';
                    if (!active) e.currentTarget.style.color = 'var(--text-primary)';
                }}
                onMouseLeave={(e) => {
                    e.currentTarget.style.background = 'transparent';
                    e.currentTarget.style.color = active ? 'var(--accent)' : 'var(--text-secondary)';
                }}
                style={{
                    gap: 3,
                    marginLeft: 'auto',
                    padding: '5px 0',
                    borderRadius: 6,
                    fontSize: 'var(--fs-xs)',
                    fontWeight: 800,
                    letterSpacing: '0.04em',
                    color: active ? 'var(--accent)' : 'var(--text-secondary)',
                    cursor: 'pointer',
                }}
            >
                {active
                    ? (sortDir === 'desc'
                        ? <ChevronDown size={13} strokeWidth={2.5} />
                        : <ChevronUp size={13} strokeWidth={2.5} />)
                    : <ChevronsUpDown size={13} style={{ opacity: 0.5 }} />}
                {label}
            </button>
        );
    };

    // Сумма СЧА (млрд ₽) по ВЫБРАННЫМ фондам набора — строка «Выбрать все» и
    // заголовки УК (аналог navSumBln в FundsTable, но по чекнутым).
    const navSumBln = (fs: FundWithHistory[]) =>
        fs.reduce((s, f) => (draft.has(f.ticker) ? s + (f.nav_rub ?? 0) : s), 0) / 1e9;

    const sortFunds = (fs: FundWithHistory[]) =>
        [...fs].sort((a, b) => {
            if (sortKey === 'y1') {
                const ya = bestReturn(a.returns)?.v ?? null;
                const yb = bestReturn(b.returns)?.v ?? null;
                if (ya == null && yb == null) return 0;
                if (ya == null) return 1;
                if (yb == null) return -1;
                return sortDir === 'desc' ? yb - ya : ya - yb;
            }
            const na = a.nav_rub ?? 0;
            const nb = b.nav_rub ?? 0;
            return sortDir === 'desc' ? nb - na : na - nb;
        });

    // Группы УК сортируем по суммарной СЧА группы (крупные УК сверху) — стабильно
    // и не зависит от выбора; внутри группы — активная колонка сортировки.
    const groups = useMemo(() => {
        const gs = groupByUk(funds);
        const navOf = (g: UkGroup) => g.funds.reduce((s, f) => s + (f.nav_rub ?? 0), 0);
        return gs.sort((a, b) => navOf(b) - navOf(a));
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [funds]);

    const allSelected = allTickers.length > 0 && allTickers.every((t) => draft.has(t));
    const anySelected = allTickers.some((t) => draft.has(t));
    const toggleAll = () => {
        setDraft(allSelected ? new Set() : new Set(allTickers));
    };
    const toggleFund = (ticker: string) => {
        setDraft((prev) => {
            const next = new Set(prev);
            if (next.has(ticker)) next.delete(ticker);
            else next.add(ticker);
            return next;
        });
    };
    const toggleGroup = (g: UkGroup) => {
        const tickers = g.funds.map((f) => f.ticker);
        const allOn = tickers.every((t) => draft.has(t));
        setDraft((prev) => {
            const next = new Set(prev);
            if (allOn) tickers.forEach((t) => next.delete(t));
            else tickers.forEach((t) => next.add(t));
            return next;
        });
    };

    return (
        <div className="fixed inset-0 z-50 flex items-start justify-center p-4 pt-8 sm:pt-10">
            <div
                className="absolute inset-0"
                style={{ backgroundColor: 'rgba(0,0,0,0.6)' }}
                onClick={applyAndClose}
            />
            <div
                className="relative w-full rounded-2xl max-h-[90vh] overflow-hidden flex flex-col"
                style={{
                    backgroundColor: 'var(--bg-secondary)',
                    border: '2px solid var(--text-primary)',
                    boxShadow: 'var(--shadow-hard-chip, 6px 6px 0 var(--text-primary))',
                    color: 'var(--text-primary)',
                    maxWidth: 816,
                }}
            >
                {/* Заголовок + счётчик выбранных в одном ряду, справа крестик —
                    как шапка модалки фондов в «Деньгах в фондах». */}
                <div
                    className="flex items-center flex-shrink-0 flex-wrap"
                    style={{ padding: 'var(--sp-4) var(--sp-5) var(--sp-3)', gap: '4px 12px', borderBottom: SOFT_BORDER }}
                >
                    <span className="font-semibold" style={{ fontSize: 'var(--fs-base)' }}>
                        Фонды акций
                    </span>
                    <span style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)' }}>
                        выбрано {draft.size} из {allTickers.length}
                    </span>
                    <button
                        onClick={applyAndClose}
                        className="p-2 -mr-2 rounded-lg transition-colors flex-shrink-0 ml-auto"
                        style={{ color: 'var(--text-secondary)' }}
                        aria-label="Закрыть"
                    >
                        <X size={22} />
                    </button>
                </div>

                {/* Скролл-зона с симметричными отступами (scrollbar-gutter both-edges —
                    вертикальный скроллбар не съедает правый отступ). */}
                <div
                    className="flex-1 min-h-0 overflow-y-auto styled-scrollbar"
                    style={{ padding: '0 var(--sp-4) var(--sp-4)', scrollbarGutter: 'stable both-edges' }}
                >
                    <table className="w-full" style={{ fontSize: 'var(--fs-sm)', tableLayout: 'fixed' }}>
                        {/* Раскладка колонок — как bare-FundsTable: чекбокс 40,
                            Название тянется, СЧА 84, Доходность 142. */}
                        <colgroup>
                            <col style={{ width: 40 }} />
                            <col />
                            <col style={{ width: 84 }} />
                            <col style={{ width: 142 }} />
                        </colgroup>
                        <thead>
                            <tr className="text-left" style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)' }}>
                                <th className="pl-2 pr-0 py-2 w-10"></th>
                                <th className="pl-1 pr-2 py-2" style={HEAD_STYLE}>Название</th>
                                <th className="px-2 py-2 text-right whitespace-nowrap">
                                    {sortButton('nav', 'СЧА', 'Стоимость чистых активов фонда, млрд ₽')}
                                </th>
                                <th className="px-2 py-2 text-right whitespace-nowrap">
                                    {sortButton('y1', 'Доходность', 'Доходность пая за 1 год; для молодых фондов — за лучший доступный период (6м/3м/1м, период подписан).')}
                                </th>
                            </tr>
                        </thead>
                        <tbody>
                            {/* Мастер-строка «Выбрать все» + суммарная СЧА выбранных. */}
                            <tr style={{ borderBottom: SOFT_BORDER }}>
                                <td className="pl-2 pr-0 py-1">
                                    <div
                                        className="flex items-center justify-center w-5 h-5 rounded-lg cursor-pointer"
                                        onClick={toggleAll}
                                    >
                                        <CheckBox checked={allSelected} indeterminate={anySelected && !allSelected} />
                                    </div>
                                </td>
                                <td className="pl-1 pr-2 py-1 cursor-pointer select-none" onClick={toggleAll}>
                                    <span className="font-bold" style={{ fontSize: 'var(--fs-sm)', color: 'var(--text-primary)' }}>Выбрать все</span>
                                </td>
                                <td className="px-2 py-1 text-right cursor-pointer select-none" style={NUM_STYLE} onClick={toggleAll}>
                                    {navSumBln(funds).toFixed(2)}
                                </td>
                                <td className="cursor-pointer select-none" onClick={toggleAll} />
                            </tr>

                            {groups.map((g) => {
                                const tickers = g.funds.map((f) => f.ticker);
                                const gAll = tickers.every((t) => draft.has(t));
                                const gAny = tickers.some((t) => draft.has(t));
                                const isCollapsed = collapsed.has(g.key);
                                const logo = (g.ukId != null ? UK_LOGOS[String(g.ukId)] : undefined) ?? UK_LOGOS[g.name];
                                return (
                                    <React.Fragment key={g.key}>
                                        {/* Заголовок УК: групповой чекбокс + шеврон-коллапс +
                                            аватар + имя + аббревиатура + СЧА группы (аналог
                                            строки подкатегории в FundsTable). Имя — тем же
                                            fs-sm, что имена фондов ниже. */}
                                        <tr style={{ borderTop: SOFT_BORDER }}>
                                            <td className="pl-2 pr-0 py-1">
                                                <div
                                                    className="flex items-center justify-center w-5 h-5 rounded-lg cursor-pointer"
                                                    onClick={() => toggleGroup(g)}
                                                >
                                                    <CheckBox checked={gAll} indeterminate={gAny && !gAll} />
                                                </div>
                                            </td>
                                            <td
                                                className="pl-1 pr-2 py-1 cursor-pointer select-none"
                                                onClick={() => setCollapsed((prev) => {
                                                    const next = new Set(prev);
                                                    next.has(g.key) ? next.delete(g.key) : next.add(g.key);
                                                    return next;
                                                })}
                                            >
                                                <div className="flex items-center gap-2 min-w-0">
                                                    {/* Индикатор сворачивания — SVG-шеврон (как у сортировки
                                                        колонок), а не текстовый ▼: тот в Windows подхватывался
                                                        эмодзи-шрифтом и выглядел инородно. */}
                                                    <ChevronDown
                                                        size={13}
                                                        strokeWidth={2.5}
                                                        className="flex-shrink-0 transition-transform duration-200"
                                                        style={{ color: 'var(--text-secondary)', transform: isCollapsed ? 'rotate(-90deg)' : 'rotate(0deg)' }}
                                                    />
                                                    <span
                                                        style={{
                                                            width: 20, height: 20, borderRadius: '50%',
                                                            display: 'flex', alignItems: 'center', justifyContent: 'center',
                                                            flexShrink: 0, overflow: 'hidden',
                                                            fontWeight: 900, fontSize: 10, lineHeight: 1,
                                                            backgroundColor: logo ? (logo.img ? undefined : logo.bg) : 'var(--bg-primary)',
                                                            color: logo?.color ?? 'var(--text-secondary)',
                                                        }}
                                                    >
                                                        {logo
                                                            ? (logo.img
                                                                ? <img src={logo.img} alt={logo.name} style={{ width: '100%', height: '100%', objectFit: 'cover' }} />
                                                                : logo.letter)
                                                            : (g.name.trim().charAt(0).toUpperCase() || '?')}
                                                    </span>
                                                    <span className="font-bold" style={{ fontSize: 'var(--fs-sm)', color: 'var(--text-primary)' }}>{g.name}</span>
                                                    {/* Аббревиатура УК — та же роль и стиль, что тикер
                                                        рядом с именем фонда в строках ниже. */}
                                                    {ukAbbr(g.ukId, g.name) && (
                                                        <span
                                                            className="flex-shrink-0"
                                                            style={{ fontSize: 'var(--fs-xs)', fontWeight: 400, color: 'var(--text-secondary)' }}
                                                        >
                                                            {ukAbbr(g.ukId, g.name)}
                                                        </span>
                                                    )}
                                                    {gAny && !gAll && (
                                                        <span style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-muted)', fontWeight: 700 }}>
                                                            {tickers.filter((t) => draft.has(t)).length}/{tickers.length}
                                                        </span>
                                                    )}
                                                </div>
                                            </td>
                                            <td className="px-2 py-1 text-right select-none" style={NUM_STYLE}>
                                                {navSumBln(g.funds).toFixed(2)}
                                            </td>
                                            <td />
                                        </tr>
                                        {!isCollapsed && sortFunds(g.funds).map((fund) => {
                                            const on = draft.has(fund.ticker);
                                            const uk = resolveFundLogo(fund.ticker, fund.uk_id);
                                            const br = bestReturn(fund.returns);
                                            return (
                                                <tr
                                                    key={fund.ticker}
                                                    className={`transition-colors cursor-pointer ${on ? 'hover:bg-white/5' : 'opacity-50 grayscale hover:bg-white/5'}`}
                                                    style={{ height: 41, borderTop: SOFT_BORDER }}
                                                    onClick={() => toggleFund(fund.ticker)}
                                                >
                                                    <td className="pl-2 pr-0 py-1">
                                                        <div className="flex items-center justify-center w-5 h-5 rounded-lg">
                                                            <CheckBox checked={on} />
                                                        </div>
                                                    </td>
                                                    <td className="pl-1 pr-2 py-1 overflow-hidden">
                                                        <div className="flex items-center gap-2 min-w-0">
                                                            {uk && (
                                                                <div
                                                                    className="rounded-full flex items-center justify-center flex-shrink-0 font-black text-sm overflow-hidden"
                                                                    style={{
                                                                        backgroundColor: uk.img ? undefined : uk.bg,
                                                                        color: uk.color,
                                                                        width: 28, height: 28,
                                                                    }}
                                                                >
                                                                    {uk.img
                                                                        ? <img src={uk.img} alt={uk.name} className="w-full h-full object-cover" />
                                                                        : uk.letter}
                                                                </div>
                                                            )}
                                                            <FadedName name={fund.name} display={stripUkName(fund.name, fund.uk_id)} />
                                                            <span
                                                                className="flex-shrink-0"
                                                                style={{ fontSize: 'var(--fs-xs)', fontWeight: 400, color: 'var(--text-secondary)' }}
                                                            >
                                                                {fund.ticker}
                                                            </span>
                                                        </div>
                                                    </td>
                                                    <td className="px-2 py-1 text-right" style={NUM_STYLE}>
                                                        {fund.nav_rub ? (fund.nav_rub / 1e9).toFixed(2) : '—'}
                                                    </td>
                                                    <td
                                                        className="px-2 py-1 text-right whitespace-nowrap"
                                                        style={{
                                                            ...NUM_STYLE,
                                                            ...(br ? { color: br.v >= 0 ? 'var(--funds-flow-positive)' : 'var(--funds-flow-negative)' } : {}),
                                                        }}
                                                    >
                                                        {!br ? '—' : (
                                                            <>
                                                                {br.v >= 0 ? '+' : ''}{br.v.toFixed(1)}%
                                                                {br.label !== '1г' && (
                                                                    <span style={{ fontSize: 'var(--fs-2xs)', marginLeft: 3, color: 'var(--text-secondary)' }}>{br.label}</span>
                                                                )}
                                                            </>
                                                        )}
                                                    </td>
                                                </tr>
                                            );
                                        })}
                                    </React.Fragment>
                                );
                            })}
                        </tbody>
                    </table>
                </div>

                {/* Футер «Готово» — применяет и закрывает (как FundPicker multi). */}
                <div
                    className="px-6 py-4 flex-shrink-0"
                    style={{ borderTop: SOFT_BORDER }}
                >
                    <button
                        type="button"
                        onClick={applyAndClose}
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
                        Готово{allSelected ? '' : ` · ${draft.size}`}
                    </button>
                </div>
            </div>
        </div>
    );
}

export interface PortfolioFundPickerProps {
    funds: FundWithHistory[];
    /** Выбранные тикеры; пусто = все фонды. */
    selected: Set<string>;
    onChange: (next: Set<string>) => void;
}

export default function PortfolioFundPicker({ funds, selected, onChange }: PortfolioFundPickerProps) {
    const [open, setOpen] = useState(false);
    const allActive = selected.size === 0;
    const active = !allActive;
    const label = allActive ? 'Все фонды акций' : `${selected.size} из ${funds.length} фондов`;

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
                <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{label}</span>
                <span style={{ fontSize: '0.75em', opacity: 0.85, flexShrink: 0 }}>▾</span>
            </button>

            {open && (
                <PickerModal
                    funds={funds}
                    selected={selected}
                    onApply={onChange}
                    onClose={() => setOpen(false)}
                />
            )}
        </div>
    );
}
