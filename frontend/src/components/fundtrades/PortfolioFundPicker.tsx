// PortfolioFundPicker — выбор конкретных фондов для «Общего портфеля».
// Дизайн-код заимствован у виджета выбора фондов в «Деньгах в фондах»
// (FundsMoneyPage fundPickerOpen-модалка + FundsTable bare): top-anchored окно
// maxWidth 816 с editorial-рамкой и hard-shadow, внутри таблица с фиксированной
// раскладкой колонок — чекбокс · Название (лого УК + имя + тикер) · СЧА ·
// Доходность, сортировка по СЧА/доходности, строка «Выбрать все» и групповые
// чекбоксы. Группы — те же подкатегории, что в «Деньгах в фондах» (Индекс
// Мосбиржи → Управляемые фонды → Авторские), в порядке subcategory_order из БД.
// Отличия от «Денег в фондах»: семантика «выбрано» (не «скрыто») и выбор,
// который применяется по закрытию окна.
//
// Индексная группа всегда последняя, а в шапке — таблетка «Без индексных фондов»
// (прожата по умолчанию): снимает эти три фонда и размывает их строки.
//
// Черновик выбора живёт внутри модалки и применяется на «Готово»/X/overlay —
// промежуточное состояние «сняты все» не улетает в API (пусто = все фонды).
// selected: Set тикеров, пусто = все (канон: полный набор схлопывается в пусто).

import React, { useEffect, useMemo, useState } from 'react';
import { X, Check, Minus, ChevronDown, ChevronUp, ChevronsUpDown, AlertCircle, Lock } from 'lucide-react';
import { resolveFundLogo, stripUkName, SUBCATEGORY_HELP, isIndexSubcategory } from '../../config/fundConfig';
import HelpTooltip from '../HelpTooltip';
import { useTierAccess } from '../../contexts/TierFeaturesContext';
import { useUpgradePrompt } from '../tier/UpgradeModal';
import type { FundWithHistory } from '../../services/api';

const SOFT_BORDER = '1px solid color-mix(in srgb, var(--text-primary) 12%, transparent)';
const NAME_FADE = 'linear-gradient(to right, #000 calc(100% - 30px), transparent 100%)';

// Высота строки фонда: фиксированные 41px + 1px SOFT_BORDER сверху. По ней
// считается высота оверлея над выключенной группой (плашка «Выключены · вернуть»).
const ROW_H = 42;

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

interface SubcatGroup {
    key: string;
    /** Подпись группы (с тем же переименованием, что в «Деньгах в фондах»). */
    label: string;
    /** Ключ справки SUBCATEGORY_HELP — исходное имя подкатегории. */
    subcat: string | null;
    order: number;
    funds: FundWithHistory[];
}

// Подпись подкатегории — как в FundsTable: «Управляемые фонды акций» в списке
// фондов акций сокращается до «Управляемые фонды» (категория и так про акции).
const subcatLabel = (subcat: string | null): string => {
    if (!subcat) return 'Прочие';
    return subcat === 'Управляемые фонды акций' ? 'Управляемые фонды' : subcat;
};

// Группировка по ПОДКАТЕГОРИЯМ (Индекс МосБиржи / Управляемые фонды / Авторские) —
// один в один с «Деньгами в фондах». Порядок групп — subcategory_order из БД (той
// же колонки, по которой сортирует /api/funds); фолбэк — порядок появления.
// Исключение: индексная группа всегда уходит В КОНЕЦ — три индексных фонда почти
// идентичны, их сделки это ребалансировка вслед за индексом, а не решения
// управляющих, ради которых смотрят портфель.
function groupBySubcat(funds: FundWithHistory[]): SubcatGroup[] {
    const map = new Map<string, SubcatGroup>();
    let seen = 0;
    for (const f of funds) {
        const key = f.subcategory ?? '__none__';
        let g = map.get(key);
        if (!g) {
            g = {
                key,
                label: subcatLabel(f.subcategory),
                subcat: f.subcategory,
                order: f.subcategory_order ?? 1000 + seen++,
                funds: [],
            };
            map.set(key, g);
        }
        g.funds.push(f);
    }
    return Array.from(map.values()).sort((a, b) => {
        const ai = isIndexSubcategory(a.subcat) ? 1 : 0;
        const bi = isIndexSubcategory(b.subcat) ? 1 : 0;
        return ai !== bi ? ai - bi : a.order - b.order;
    });
}

const MONTHS_LOWER = ['январь', 'февраль', 'март', 'апрель', 'май', 'июнь',
    'июль', 'август', 'сентябрь', 'октябрь', 'ноябрь', 'декабрь'];

// «2026-07-31» → «июль 2026». Месяц берём из строки, а не через Date: ISO-дата
// парсится как UTC и в западных таймзонах month-end съезжает на месяц назад.
const monthYearLower = (iso: string): string =>
    `${MONTHS_LOWER[Number(iso.slice(5, 7)) - 1]} ${iso.slice(0, 4)}`;
const monthKey = (iso: string): string => iso.slice(0, 7);

/** Тикеры индексных фондов набора — их снимает тумблер «Без индексных фондов». */
export const indexFundTickers = (funds: FundWithHistory[]): string[] =>
    funds.filter((f) => isIndexSubcategory(f.subcategory)).map((f) => f.ticker);

/** Дефолтный набор портфеля: всё, кроме индексных фондов. */
export const defaultPortfolioTickers = (funds: FundWithHistory[]): string[] =>
    funds.filter((f) => !isIndexSubcategory(f.subcategory)).map((f) => f.ticker);

export const INDEX_FUNDS_HELP =
    'Индексные фонды механически повторяют индекс Мосбиржи: составы у них практически '
    + 'одинаковые, а сделки — техническая ребалансировка вслед за индексом, а не решения '
    + 'управляющих. Поэтому по умолчанию они не входят в общий портфель. Выключите тумблер, '
    + 'если хотите видеть весь рынок фондов акций, включая индексные.';

// ── Модалка (шелл — fundPickerOpen из FundsMoneyPage) ────────────────────────
function PickerModal({
    funds,
    selected,
    targetMonth,
    excludedTickers,
    title,
    onApply,
    onClose,
}: {
    funds: FundWithHistory[];
    selected: Set<string>;
    targetMonth?: string | null;
    excludedTickers?: Set<string>;
    title: string;
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
        // Пустой черновик для API неотличим от «все фонды», поэтому сводим его к
        // пулу доступных: пока таблетка прожата, индексные не должны вернуться
        // «через ноль» (снял все → вернулись все 19, включая индексные).
        const effective = draft.size === 0 ? new Set(pool) : draft;
        const covered = effective.size === 0 || allTickers.every((t) => effective.has(t));
        onApply(covered ? new Set() : new Set(effective));
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

    // Группы — подкатегории в порядке из БД; внутри группы фонды сортируются
    // активной колонкой (СЧА / доходность), как в «Деньгах в фондах».
    const groups = useMemo(() => groupBySubcat(funds), [funds]);

    // Тумблер «Без индексных фондов» — не отдельное состояние, а срез выбора:
    // включён ⇔ ни один индексный фонд не отмечен. Поэтому ручная галка на
    // индексном фонде сама его выключает, без рассинхрона.
    const idxTickers = useMemo(() => indexFundTickers(funds), [funds]);
    const indexOff = idxTickers.length > 0 && idxTickers.every((t) => !draft.has(t));
    const toggleIndexOff = () => {
        setDraft((prev) => {
            const next = new Set(prev);
            if (indexOff) idxTickers.forEach((t) => next.add(t));
            else idxTickers.forEach((t) => next.delete(t));
            return next;
        });
    };

    // «Выбрать все» подчиняется таблетке: пока индексные выключены, пул выбора —
    // только доступные фонды, и мастер-чекбокс их не воскрешает.
    const pool = useMemo(() => {
        if (!indexOff) return allTickers;
        const idx = new Set(idxTickers);
        return allTickers.filter((t) => !idx.has(t));
    }, [indexOff, allTickers, idxTickers]);

    // Самый свежий месяц, за который вообще есть составы в наборе. Знак «!» висит
    // относительно НЕГО, а не относительно выбранного месяца: «у фонда ещё не
    // вышли последние данные» — свойство фонда, оно не должно мигать при листании
    // истории. Тир-задержка уже учтена: last_snapshot_date приходит с бэка
    // обрезанным по cutoff тарифа.
    const freshestMonth = useMemo(() => {
        let m: string | null = null;
        for (const f of funds) {
            const d = f.last_snapshot_date;
            if (d && (!m || d > m)) m = d;
        }
        return m;
    }, [funds]);

    // Знак «!» — тот же приём, что в списке фондов «Денег в фондах»:
    //  • фонд отстал от свежего месяца (последние данные ещё не вышли), либо
    //  • фонд не отчитался за ВЫБРАННЫЙ месяц и потому не вошёл в портфель
    //    (excluded_funds из /portfolio — ловит и дырки в середине истории).
    const staleInfo = (f: FundWithHistory): string | null => {
        const last = f.last_snapshot_date;
        const behind = !!freshestMonth
            && (!last || monthKey(last) < monthKey(freshestMonth));
        if (behind) {
            const tail = last
                ? `Последний доступный состав — ${monthYearLower(last)}.`
                : 'Опубликованных составов пока нет.';
            return `Состав за ${monthYearLower(freshestMonth!)} ещё не опубликован. ${tail}`;
        }
        if (targetMonth && excludedTickers?.has(f.ticker)) {
            return `Состава за ${monthYearLower(targetMonth)} у фонда нет,`
                + ' поэтому в портфель этого месяца он не включён.';
        }
        return null;
    };

    const allSelected = pool.length > 0 && pool.every((t) => draft.has(t));
    const anySelected = pool.some((t) => draft.has(t));
    const toggleAll = () => {
        setDraft((prev) => {
            const next = new Set(prev);
            if (allSelected) pool.forEach((t) => next.delete(t));
            else pool.forEach((t) => next.add(t));
            return next;
        });
    };
    const toggleFund = (ticker: string) => {
        setDraft((prev) => {
            const next = new Set(prev);
            if (next.has(ticker)) next.delete(ticker);
            else next.add(ticker);
            return next;
        });
    };
    const toggleGroup = (g: SubcatGroup) => {
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
                        {title}
                    </span>
                    {/* Знаменатель — доступные к выбору фонды: с прожатой таблеткой
                        индексные из пула выпадают (19 → 16). Сколько именно выключено,
                        тут не пишем: об этом говорят сама прожатая таблетка и плашка
                        поверх размытой группы. */}
                    <span style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)' }}>
                        выбрано {draft.size} из {allTickers.length - (indexOff ? idxTickers.length : 0)}
                    </span>
                    {/* Таблетка-тумблер «Без индексных фондов» прямо в шапке, следом за
                        счётчиком: прожата — три индексных фонда сняты (счётчик сразу
                        меньше на 3), рядом «?» с объяснением почему. */}
                    {idxTickers.length > 0 && (
                        <span className="inline-flex items-center flex-shrink-0" style={{ gap: 6 }}>
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
                            {/* float — окно модалки с overflow:hidden обрезало бы
                                поповер по своему краю (особенно на узком экране). */}
                            <HelpTooltip content={INDEX_FUNDS_HELP} size={16} float />
                        </span>
                    )}
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
                                return (
                                    <React.Fragment key={g.key}>
                                        {/* Заголовок подкатегории — один в один со строкой
                                            подкатегории в FundsTable: чекбокс группы, шеврон
                                            сворачивания, название, «?»-справка и СЧА группы. */}
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
                                                    <span className="font-bold" style={{ fontSize: 'var(--fs-sm)', color: 'var(--text-primary)' }}>
                                                        {g.label}
                                                    </span>
                                                    {/* Справка по подкатегории — та же, что в «Деньгах в
                                                        фондах»; клик по «?» не сворачивает группу. */}
                                                    {g.subcat && SUBCATEGORY_HELP[g.subcat] && (
                                                        <span className="inline-flex" onClick={(e) => e.stopPropagation()}>
                                                            <HelpTooltip content={SUBCATEGORY_HELP[g.subcat]} size={18} float />
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
                                            // Пока таблетка прожата, индексные фонды слегка
                                            // размыты — видно, что они выведены из портфеля,
                                            // но строка остаётся кликабельной.
                                            const blurred = indexOff && isIndexSubcategory(fund.subcategory);
                                            const stale = staleInfo(fund);
                                            return (
                                                <tr
                                                    key={fund.ticker}
                                                    className={`transition-colors cursor-pointer ${on ? 'hover:bg-white/5' : 'opacity-50 grayscale hover:bg-white/5'}`}
                                                    style={{
                                                        height: 41,
                                                        borderTop: SOFT_BORDER,
                                                        ...(blurred ? { filter: 'blur(1.1px)' } : null),
                                                    }}
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
                                                            <FadedName
                                                                name={fund.name}
                                                                display={stripUkName(fund.name, fund.uk_id)}
                                                            />
                                                            <span
                                                                className="flex-shrink-0"
                                                                style={{ fontSize: 'var(--fs-xs)', fontWeight: 400, color: 'var(--text-secondary)' }}
                                                            >
                                                                {fund.ticker}
                                                            </span>
                                                            {/* Фонд не отчитался за месяц среза — «!» с месяцем
                                                                его последнего состава (как в списке фондов
                                                                «Денег в фондах»). */}
                                                            {stale && (
                                                                <span
                                                                    className="cursor-help inline-flex flex-shrink-0"
                                                                    style={{ opacity: 0.6, color: 'var(--text-secondary)' }}
                                                                    title={stale}
                                                                    onClick={(e) => e.stopPropagation()}
                                                                >
                                                                    <AlertCircle size={17} strokeWidth={2.2} />
                                                                </span>
                                                            )}
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
                                        {/* Плашка «выключено» лежит ПОВЕРХ всей размытой группы,
                                            а не рядом с её названием: так метка относится ко всем
                                            строкам сразу и не читается как свойство подкатегории.
                                            Технически — нулевой высоты строка сразу за группой;
                                            оверлей внутри её ячейки растягивается вверх на высоту
                                            группы (строки фиксированные, ROW_H), поэтому плашка
                                            встаёт ровно по центру блока. Клик возвращает фонды. */}
                                        {!isCollapsed && indexOff && isIndexSubcategory(g.subcat) && (
                                            <tr style={{ height: 0 }}>
                                                <td colSpan={4} style={{ padding: 0, border: 0, height: 0, position: 'relative' }}>
                                                    <div
                                                        style={{
                                                            position: 'absolute', left: 0, right: 0, bottom: 0,
                                                            height: g.funds.length * ROW_H,
                                                            display: 'flex', alignItems: 'center', justifyContent: 'center',
                                                            pointerEvents: 'none',
                                                        }}
                                                    >
                                                        <button
                                                            type="button"
                                                            onClick={toggleIndexOff}
                                                            className="editorial-press"
                                                            title="Индексные фонды выключены таблеткой в шапке. Нажмите, чтобы вернуть их в портфель."
                                                            style={{
                                                                pointerEvents: 'auto',
                                                                padding: '5px 16px',
                                                                borderRadius: 999,
                                                                border: '2px solid var(--text-primary)',
                                                                background: 'var(--bg-secondary)',
                                                                color: 'var(--text-primary)',
                                                                fontSize: 'var(--fs-xs)',
                                                                fontWeight: 700,
                                                                whiteSpace: 'nowrap',
                                                                cursor: 'pointer',
                                                                boxShadow: '3px 3px 0 var(--text-primary)',
                                                            }}
                                                        >
                                                            Выключены · вернуть
                                                        </button>
                                                    </div>
                                                </td>
                                            </tr>
                                        )}
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
    /** Месяц среза портфеля (ISO): по нему помечаем отставшие фонды знаком «!». */
    targetMonth?: string | null;
    /** Тикеры, не опубликовавшие состав за месяц среза (excluded_funds из /portfolio). */
    excludedTickers?: Set<string>;
    /** Заголовок модалки. По умолчанию — весь набор фондов акций. */
    title?: string;
    /** Подпись таблетки, когда выбран весь пул. */
    allLabel?: string;
    /** Сбрасывать сохранённую подвыборку, если тариф больше не даёт пикер.
     *  Включать там, где `selected` персистится и уезжает на бэкенд параметром
     *  `funds` («Общий портфель»). НЕ включать во вкладке «По бумаге»: там
     *  «выбрано» — это производная от дефолта «без индексных фондов», и сброс
     *  в пусто не вернул бы состояние по умолчанию, а сломал бы его. */
    resetWhenLocked?: boolean;
}

export default function PortfolioFundPicker({
    funds, selected, onChange, targetMonth, excludedTickers,
    title = 'Фонды акций', allLabel = 'Все фонды акций',
    resetWhenLocked = false,
}: PortfolioFundPickerProps) {
    const [open, setOpen] = useState(false);

    // Выбор своего пула фондов — с Basic (матрица fund_trades.fund_picker,
    // решение владельца 2026-08-09). Гейт живёт ЗДЕСЬ, в самом пикере: он один
    // на все три поверхности — «Общий портфель», «По бумаге» и мобильный sheet
    // «Опции». «Витрины» это не касается — там пикера нет.
    // `isLoading ||` — пока тариф не резолвнут, замок не вешаем.
    const access = useTierAccess('fund_trades');
    const { showUpgrade } = useUpgradePrompt();
    const canPick = access.isLoading || access.canUseFlag('fund_picker');

    // Санитайз слетевшего с тарифа: подвыборка «Общего портфеля» персистится
    // (frame:fundtrades:portfolioFunds) и переживает окончание подписки. Бэкенд
    // такой `funds` уже игнорирует — оставить набор применённым значило бы
    // рисовать таблетку «15 из 19 фондов» под данными по всем 19.
    useEffect(() => {
        if (!resetWhenLocked || access.isLoading || canPick) return;
        if (selected.size > 0) onChange(new Set());
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [resetWhenLocked, access.isLoading, canPick, selected.size]);
    // Подпись таблетки читает тумблер «Без индексных фондов» тем же способом, что и
    // сама модалка (индексные сняты ⇔ тумблер прожат): пул выбора — только доступные
    // фонды. Взят весь пул целиком (в том числе дефолт «всё, кроме индексных») — это
    // «Все фонды акций», ручной отбор внутри пула — «15 из 16 фондов». Знаменатель
    // по всему набору (19) писал бы «16 из 19», будто три фонда сняты вручную.
    const idxTickers = useMemo(() => indexFundTickers(funds), [funds]);
    const indexOff = idxTickers.length > 0 && idxTickers.every((t) => !selected.has(t));
    const pool = funds.length - (indexOff ? idxTickers.length : 0);
    const allActive = selected.size === 0 || selected.size === pool;
    // Заперт → таблетка всегда в «нейтральном» виде: акцентная заливка означает
    // «фильтр применён», а у locked-тира он не применён и применён быть не может.
    const active = !allActive && canPick;
    const label = allActive || !canPick ? allLabel : `${selected.size} из ${pool} фондов`;

    return (
        <div style={{ display: 'inline-flex', minWidth: 0 }}>
            <button
                type="button"
                // Locked: модалку не открываем вовсе (честнее блюра — под ним
                // всё равно лежал бы обычный публичный список фондов, а платная
                // тут не «таблица», а сама возможность собрать свой пул).
                onClick={() => {
                    if (!canPick) {
                        showUpgrade({
                            tier: access.requiredTierFor({ flag: 'fund_picker' }) ?? 'basic',
                            featureName: 'выбор фондов',
                            indicator: 'fund_trades',
                        });
                        return;
                    }
                    setOpen(true);
                }}
                title={canPick ? undefined : 'Выбор фондов — на тарифе Basic и выше'}
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
                {canPick
                    ? <span style={{ fontSize: '0.75em', opacity: 0.85, flexShrink: 0 }}>▾</span>
                    : <Lock size={13} strokeWidth={2.4} style={{ flexShrink: 0, opacity: 0.85 }} />}
            </button>

            {open && canPick && (
                <PickerModal
                    funds={funds}
                    selected={selected}
                    targetMonth={targetMonth}
                    excludedTickers={excludedTickers}
                    title={title}
                    onApply={onChange}
                    onClose={() => setOpen(false)}
                />
            )}
        </div>
    );
}
