import React from 'react';
import { ArrowDown, ArrowUp, Lock, AlertCircle, Check, Minus, ChevronDown, ChevronUp, ChevronsUpDown } from 'lucide-react';
import { resolveFundLogo, stripUkName, SUBCATEGORY_HELP } from '../../config/fundConfig';
import HelpTooltip from '../HelpTooltip';
import type { FundInfo, FundsChartResponse } from '../../services/api';
import { useUpgradePrompt } from '../tier/UpgradeModal';
import { useTierAccess } from '../../contexts/TierFeaturesContext';

const FUND_COLORS = [
    '#2EE59D', '#4DA3FF', '#9D4DFF', '#FF4D4D', '#FFB020',
    '#00D9FF', '#FF6B9D', '#FCD34D', '#14B8A6', '#F97316'
];

// Разделитель строк в bare-режиме (модалка) — тонкая линия, как между активами
// в поиске ОИ (.instrument-list .instrument-item + .instrument-item в index.css).
// Дефолтный editorial `border-theme` = 2px solid var(--text-primary), т.е. жирная
// чёрная полоса — в списке фондов она выглядит грубо.
const SOFT_BORDER = '1px solid color-mix(in srgb, var(--text-primary) 12%, transparent)';

// Стиль чисел в столбцах СЧА/Доходность (bare) — как значения в поиске ОИ
// (InstrumentSearchModal): вес 600, табличные цифры, обычный шрифт (не mono).
// Заголовки колонок этим не трогаем.
const OI_NUM_STYLE: React.CSSProperties = {
    fontWeight: 600,
    fontVariantNumeric: 'tabular-nums',
};

// Числа СЧА — приглушённо-серые (var(--text-secondary)), как столбец «Объём» в
// поиске ОИ, а не насыщенно-чёрные. Доходность остаётся семантически цветной.
const OI_NUM_SCHA_STYLE: React.CSSProperties = {
    ...OI_NUM_STYLE,
    color: 'var(--text-secondary)',
};

// Типографика заголовков колонок в bare-режиме — как в шапке поиска ОИ
// (renderSortHeader в InstrumentSearchModal, замер: 12.5px / 800 / uppercase /
// letter-spacing 0.04em). fontSize наследуется от tr (fs-xs).
const OI_HEAD_STYLE: React.CSSProperties = {
    fontWeight: 800,
    textTransform: 'uppercase',
    letterSpacing: '0.04em',
    color: 'var(--text-secondary)',
};

// Чекбокс bare-режима — акцентный скруглённый квадрат с белой галочкой, как в
// макете модалки и в мультивыборе поиска ОИ (Check/Minus вместо нативного input).
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

// Подкатегории, у которых данные (NAV) ещё наливаются — показываем бейдж
// «Скоро» на заголовке группы. Убрать имя отсюда, когда NAV появится.
const COMING_SOON_SUBCATS = new Set<string>([]);

// Лучшая доступная доходность: длиннейший период с данными (1г→6м→3м→1м).
// Совпадает с «Покупки фондов» (bestReturn) — молодые фонды (<1 года) показывают
// 6м/3м/1м с подписью периода вместо «—». null = совсем нет истории / битые данные.
function bestReturn(r?: FundInfo['returns']): { v: number; label: string } | null {
    if (!r) return null;
    if (r.y1 != null) return { v: r.y1, label: '1г' };
    if (r.m6 != null) return { v: r.m6, label: '6м' };
    if (r.m3 != null) return { v: r.m3, label: '3м' };
    if (r.m1 != null) return { v: r.m1, label: '1м' };
    return null;
}

interface FundsTableProps {
    data: FundsChartResponse | null;
    hiddenFunds: Set<number>;
    collapsedSubcats: Set<string>;
    navSortDir: 'desc' | 'asc';
    aggregatedData: { chartData: { time: string; value: number }[]; totalCurrentNav: number };
    onToggleFundVisibility: (fundId: number) => void;
    onSetHiddenFunds: React.Dispatch<React.SetStateAction<Set<number>>>;
    onSetCollapsedSubcats: React.Dispatch<React.SetStateAction<Set<string>>>;
    onSetNavSortDir: React.Dispatch<React.SetStateAction<'desc' | 'asc'>>;
    onOpenFundCard: (fund: FundInfo) => void;
    /** Bare-режим для рендера внутри модалки-виджета: без внешней editorial-рамки
     *  (border/тень/скругление/mt-6) — её даёт родитель-модалка. */
    bare?: boolean;
}

export default function FundsTable({
    data,
    hiddenFunds,
    collapsedSubcats,
    navSortDir,
    aggregatedData,
    onToggleFundVisibility,
    onSetHiddenFunds,
    onSetCollapsedSubcats,
    onSetNavSortDir,
    onOpenFundCard,
    bare = false,
}: FundsTableProps) {
    const { showUpgrade } = useUpgradePrompt();
    const fundsAccess = useTierAccess('funds_money');
    // Колонка сортировки: СЧА (nav) или доходность за 1 год (y1). Направление —
    // общий navSortDir. Клик по активной колонке инвертирует, по другой — desc.
    const [sortKey, setSortKey] = React.useState<'nav' | 'y1'>('nav');
    const toggleSort = (key: 'nav' | 'y1') => {
        if (sortKey === key) onSetNavSortDir(d => d === 'desc' ? 'asc' : 'desc');
        else { setSortKey(key); onSetNavSortDir('desc'); }
    };

    // Заголовок сортируемой колонки. В bare-режиме (модалка) — стиль заимствован
    // у поиска ОИ (renderSortHeader в InstrumentSearchModal): uppercase, 800,
    // fs-xs, letter-spacing, активная колонка = accent, неактивная = secondary,
    // трейлинг-шеврон (двойной ChevronsUpDown = «можно сортировать»). Иконка слева
    // от текста — текст прижат к правому краю колонки, как в ОИ.
    const renderSortButton = (col: 'nav' | 'y1', label: string, title?: string) => {
        const active = sortKey === col;
        if (!bare) {
            return (
                <button
                    onClick={() => toggleSort(col)}
                    title={title}
                    className="inline-flex items-center gap-1 hover:text-theme-primary transition-colors"
                >
                    {label}
                    {active && (navSortDir === 'desc'
                        ? <ArrowDown size={15} strokeWidth={2.4} />
                        : <ArrowUp size={15} strokeWidth={2.4} />)}
                </button>
            );
        }
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
                    ? (navSortDir === 'desc'
                        ? <ChevronDown size={13} strokeWidth={2.5} />
                        : <ChevronUp size={13} strokeWidth={2.5} />)
                    : <ChevronsUpDown size={13} style={{ opacity: 0.5 }} />}
                {label}
            </button>
        );
    };
    // Общая дата данных (максимальный trade_date NAV среди фондов) — выводится в шапке;
    // в строках дата остаётся только у фондов, чья СЧА отстаёт от неё (stale NAV).
    // laggardDate — самая ранняя дата среди НЕ-locked фондов (тех же, что получают
    // значок "!"): по ней определяем флаг hasStaleFunds (есть ли отстающие фонды).
    let maxDate = '';
    let laggardDate = '';
    for (const f of data?.funds ?? []) {
        const d = f.data[f.data.length - 1]?.date;
        if (!d) continue;
        if (d > maxDate) maxDate = d;
        if (f.tier_locked !== true && (!laggardDate || d < laggardDate)) laggardDate = d;
    }
    const hasStaleFunds = !!(laggardDate && maxDate && laggardDate < maxDate);
    const fmtDate = (iso: string) => iso.split('-').reverse().join('.');

    // Суммарная СЧА (млрд ₽) выбранных фондов из набора — для строки «Выбрать все»
    // и заголовков подкатегорий. Считаем только видимые (не скрытые) и не-locked,
    // чтобы сумма по группам сходилась с общей «СЧА выбранных».
    const navSumBln = (funds: FundInfo[]) =>
        funds.reduce(
            (s, f) => (hiddenFunds.has(f.fund_id) || f.tier_locked === true)
                ? s
                : s + (f.data[f.data.length - 1]?.nav ?? 0),
            0,
        ) / 1e9;

    // Мастер-переключатель «Выбрать все» над всеми подкатегориями. Семантика
    // select-all: если выбраны все — клик снимает все; иначе (частично/никого) —
    // выбирает все. Идёт по тем же fund_id, что и чекбоксы подкатегорий.
    const allFundIds = (data?.funds ?? []).map(f => f.fund_id);
    const allSelected = allFundIds.length > 0 && allFundIds.every(id => !hiddenFunds.has(id));
    const anySelected = allFundIds.some(id => !hiddenFunds.has(id));
    const toggleAllFunds = () => {
        onSetHiddenFunds(prev => {
            const next = new Set(prev);
            if (allSelected) allFundIds.forEach(id => next.add(id));      // всё выбрано → снять все
            else allFundIds.forEach(id => next.delete(id));              // не всё выбрано → выбрать все
            return next;
        });
    };

    return (
        <div
            className={bare ? '' : 'mt-6 rounded-2xl overflow-hidden editorial-frame'}
            style={bare ? undefined : { background: 'var(--bg-secondary)', padding: 0 }}
        >
            {/* Шапка только для карточного (не-bare) режима. В модалке заголовок и
                дата данных живут в шапке самой модалки (FundsMoneyPage), одной
                строкой — здесь ничего не рендерим, список начинается сразу. */}
            {!bare && (
                <div
                    className="border-b border-theme flex items-center justify-between flex-wrap"
                    style={{ padding: 'var(--sp-3) var(--sp-4)', gap: 'var(--sp-1) var(--sp-3)' }}
                >
                    <div className="flex items-baseline" style={{ gap: 'var(--sp-2)' }}>
                        <h3 className="font-semibold" style={{ fontSize: 'var(--fs-base)' }}>Фонды категории</h3>
                        {maxDate && (
                            <span className="text-theme-secondary" style={{ fontSize: 'var(--fs-xs)' }}>
                                данные на {fmtDate(maxDate)}
                                {hasStaleFunds && (
                                    <>
                                        , часть фондов запаздывает (отмечены{' '}
                                        <AlertCircle
                                            size={13}
                                            strokeWidth={2.2}
                                            style={{ display: 'inline', verticalAlign: '-0.15em', opacity: 0.6 }}
                                        />
                                        )
                                    </>
                                )}
                            </span>
                        )}
                    </div>
                    <div className="flex items-center" style={{ gap: 'var(--sp-2)' }}>
                        <span className="text-theme-secondary" style={{ fontSize: 'var(--fs-sm)' }}>Суммарная СЧА выбранных:</span>
                        <span className="font-mono font-bold" style={{ color: 'var(--funds-flow-positive)', fontSize: 'var(--fs-sm)' }}>
                            {aggregatedData.totalCurrentNav.toFixed(2)} млрд ₽
                        </span>
                    </div>
                </div>
            )}
            <div className="overflow-x-auto">
                <table className="w-full" style={{ fontSize: 'var(--fs-sm)', ...(bare ? { tableLayout: 'fixed' as const } : {}) }}>
                    {/* Bare (модалка): фиксированная раскладка колонок — Название
                        тянется и обрезается многоточием, а Тикер/СЧА/Доходность
                        прижаты к правому краю компактным блоком.
                        Ширины подобраны по замерам: активный заголовок сортировки
                        (текст + стрелка) занимает ~100px, самый длинный тикер —
                        ISIN облигационных фондов (RU000A109SR1, ~80px текста).
                        Колонка чекбокса 40px даёт симметричные отступы слева и
                        до логотипа (по 15px). */}
                    {bare && (
                        <colgroup>
                            <col style={{ width: 40 }} />
                            <col />
                            <col style={{ width: 88 }} />
                            {/* Ширины под uppercase-заголовки-кнопки с шевроном (замер
                                живой кнопки): «ДОХОДНОСТЬ» = 116px, «СЧА, МЛРД ₽» ~109px.
                                Колонка = кнопка + px-2 с двух сторон (16) + запас под
                                более широкий рендер шрифта (Яндекс). Уже — кнопка
                                вылезает за ячейку, и хвост («Ь») обрезается. */}
                            <col style={{ width: 136 }} />
                            <col style={{ width: 142 }} />
                        </colgroup>
                    )}
                    <thead>
                        <tr className="text-theme-secondary text-left" style={bare ? { fontSize: 'var(--fs-xs)' } : undefined}>
                            <th className={`${bare ? 'pl-2' : 'pl-4'} pr-0 py-2 font-medium w-10`}></th>
                            {/* bare: все заголовки — типографика шапки поиска ОИ
                                (uppercase, 800, fs-xs, letter-spacing). Название и
                                Тикер не сортируются — просто текст text-secondary. */}
                            <th className={`pl-1 ${bare ? 'pr-2' : 'pr-4 font-medium'} py-2`} style={bare ? OI_HEAD_STYLE : undefined}>Название</th>
                            <th className={`${bare ? 'px-2 text-right' : 'px-4 font-medium'} py-2`} style={bare ? OI_HEAD_STYLE : undefined}>Тикер</th>
                            <th className={`${bare ? 'px-2' : 'px-4 font-medium'} py-2 text-right whitespace-nowrap`}>
                                {renderSortButton('nav', 'СЧА, млрд ₽')}
                            </th>
                            <th className={`${bare ? 'px-2' : 'px-4 font-medium'} py-2 text-right whitespace-nowrap`}>
                                {renderSortButton('y1', 'Доходность', 'Доходность по СЧА на пай (с учётом выплат дохода). За 1 год; для молодых фондов — за лучший доступный период (6м/3м/1м, период подписан).')}
                            </th>
                        </tr>
                    </thead>
                    <tbody>
                        {allFundIds.length > 0 && (
                            <tr className={bare ? '' : 'border-b border-theme'} style={bare ? { borderBottom: SOFT_BORDER } : { background: 'color-mix(in srgb, var(--text-primary) 6%, transparent)' }}>
                                <td className={`${bare ? 'pl-2' : 'pl-4'} pr-0 py-1`}>
                                    <div
                                        className="flex items-center justify-center w-5 h-5 rounded-lg hover:bg-white/5 cursor-pointer transition-colors"
                                        onClick={toggleAllFunds}
                                    >
                                        {bare ? (
                                            <CheckBox checked={allSelected} indeterminate={anySelected && !allSelected} />
                                        ) : (
                                        <input
                                            type="checkbox"
                                            checked={allSelected}
                                            ref={el => { if (el) el.indeterminate = anySelected && !allSelected; }}
                                            onChange={() => {}}
                                            className="w-3 h-3 rounded border-theme cursor-pointer"
                                            style={{ accentColor: 'var(--accent)' }}
                                        />
                                        )}
                                    </div>
                                </td>
                                <td colSpan={bare ? 2 : 4} className="pl-1 pr-2 py-1 cursor-pointer select-none" onClick={toggleAllFunds}>
                                    <span className="text-sm font-bold text-theme-primary">Выбрать все</span>
                                </td>
                                {bare && (
                                    <>
                                        <td
                                            className="px-2 py-1 text-right cursor-pointer select-none"
                                            style={OI_NUM_SCHA_STYLE}
                                            onClick={toggleAllFunds}
                                        >
                                            {navSumBln(data?.funds ?? []).toFixed(2)}
                                        </td>
                                        <td className="cursor-pointer select-none" onClick={toggleAllFunds} />
                                    </>
                                )}
                            </tr>
                        )}
                        {(() => {
                            if (!data?.funds) return null;
                            // Группируем фонды по подкатегории
                            const groups: { subcat: string | null; funds: typeof data.funds }[] = [];
                            const subcatMap = new Map<string | null, typeof data.funds>();
                            for (const fund of data.funds) {
                                const key = fund.subcategory || null;
                                if (!subcatMap.has(key)) subcatMap.set(key, []);
                                subcatMap.get(key)!.push(fund);
                            }
                            subcatMap.forEach((funds, subcat) => groups.push({
                                subcat,
                                funds: [...funds].sort((a, b) => {
                                    if (sortKey === 'y1') {
                                        // Сортировка по лучшей доступной доходности (как в «Покупках»).
                                        const ya = bestReturn(a.returns)?.v ?? null;
                                        const yb = bestReturn(b.returns)?.v ?? null;
                                        // фонды совсем без доходности — всегда в конце
                                        if (ya == null && yb == null) return 0;
                                        if (ya == null) return 1;
                                        if (yb == null) return -1;
                                        return navSortDir === 'desc' ? yb - ya : ya - yb;
                                    }
                                    const navA = a.data[a.data.length - 1]?.nav ?? 0;
                                    const navB = b.data[b.data.length - 1]?.nav ?? 0;
                                    return navSortDir === 'desc' ? navB - navA : navA - navB;
                                }),
                            }));

                            let globalIdx = 0;
                            return groups.map(({ subcat, funds: groupFunds }) => {
                                const groupIds = groupFunds.map(f => f.fund_id);
                                const allHidden = groupIds.every(id => hiddenFunds.has(id));
                                const someHidden = groupIds.some(id => hiddenFunds.has(id));

                                const toggleSubcat = () => {
                                    onSetHiddenFunds(prev => {
                                        const next = new Set(prev);
                                        if (allHidden) {
                                            groupIds.forEach(id => next.delete(id));
                                        } else {
                                            groupIds.forEach(id => next.add(id));
                                        }
                                        return next;
                                    });
                                };

                                const isCollapsed = subcat ? collapsedSubcats.has(subcat) : false;
                                const toggleCollapse = () => {
                                    if (!subcat) return;
                                    onSetCollapsedSubcats(prev => {
                                        const next = new Set(prev);
                                        next.has(subcat) ? next.delete(subcat) : next.add(subcat);
                                        return next;
                                    });
                                };

                                return (
                                    <React.Fragment key={subcat || '__none__'}>
                                        {subcat && (
                                            <tr className={bare ? '' : 'border-t-2 border-theme'}
                                                style={bare ? { borderTop: SOFT_BORDER } : { background: 'color-mix(in srgb, var(--text-primary) 4%, transparent)' }}>
                                                <td className={`${bare ? 'pl-2' : 'pl-4'} pr-0 py-1`}>
                                                    <div
                                                        className="flex items-center justify-center w-5 h-5 rounded-lg hover:bg-white/5 cursor-pointer transition-colors"
                                                        onClick={toggleSubcat}
                                                    >
                                                        {bare ? (
                                                            <CheckBox checked={!allHidden} indeterminate={someHidden && !allHidden} />
                                                        ) : (
                                                        <input
                                                            type="checkbox"
                                                            checked={!allHidden}
                                                            ref={el => { if (el) el.indeterminate = someHidden && !allHidden; }}
                                                            onChange={() => {}}
                                                            className="w-3 h-3 rounded border-theme cursor-pointer"
                                                            style={{ accentColor: 'var(--accent)' }}
                                                        />
                                                        )}
                                                    </div>
                                                </td>
                                                <td colSpan={bare ? 2 : 4} className="pl-1 pr-2 py-1 cursor-pointer select-none" onClick={toggleCollapse}>
                                                    <div className="flex items-center gap-2">
                                                        <span className="text-[10px] text-theme-secondary transition-transform duration-200" style={{ transform: isCollapsed ? 'rotate(-90deg)' : 'rotate(0deg)' }}>▼</span>
                                                        <span className="text-sm font-bold text-theme-primary">
                                                            {subcat}
                                                        </span>
                                                        <span className="text-xs text-theme-secondary">({groupFunds.length})</span>
                                                        {subcat && SUBCATEGORY_HELP[subcat] && (
                                                            <span className="inline-flex" onClick={(e) => e.stopPropagation()}>
                                                                <HelpTooltip content={SUBCATEGORY_HELP[subcat]} size={18} />
                                                            </span>
                                                        )}
                                                        {subcat && COMING_SOON_SUBCATS.has(subcat) && (
                                                            <span className="rounded-full shrink-0" style={{ fontSize: 'var(--fs-2xs)', padding: 'calc(var(--sp-1)) var(--sp-2)', background: 'var(--accent)', color: 'var(--text-inverse)', fontWeight: 700 }}>
                                                                Скоро
                                                            </span>
                                                        )}
                                                    </div>
                                                </td>
                                                {bare && (
                                                    <>
                                                        <td
                                                            className="px-2 py-1 text-right cursor-pointer select-none"
                                                            style={OI_NUM_SCHA_STYLE}
                                                            onClick={toggleCollapse}
                                                        >
                                                            {navSumBln(groupFunds).toFixed(2)}
                                                        </td>
                                                        <td className="cursor-pointer select-none" onClick={toggleCollapse} />
                                                    </>
                                                )}
                                            </tr>
                                        )}
                                        {isCollapsed && (() => { globalIdx += groupFunds.length; return null; })()}
                                        {!isCollapsed && groupFunds.map((fund) => {
                                            const colorIdx = globalIdx++;
                                            const lastData = fund.data[fund.data.length - 1];
                                            const isHidden = hiddenFunds.has(fund.fund_id);
                                            const isLocked = fund.tier_locked === true;

                                            // Click-handler для locked → upgrade modal вместо
                                            // открытия FundCard (нет данных, нечего смотреть).
                                            const handleLockedClick = (e: React.MouseEvent) => {
                                                e.stopPropagation();
                                                const requiredTier = fundsAccess.requiredTierFor({});
                                                showUpgrade({
                                                    tier: requiredTier || 'basic',
                                                    featureName: `фонд ${fund.name} (${fund.ticker})`,
                                                    indicator: 'funds_money',
                                                });
                                            };

                                            return (
                                                <tr
                                                    key={fund.fund_id}
                                                    className={`${bare ? '' : 'border-t border-theme'} transition-colors ${
                                                        isLocked ? 'cursor-not-allowed' :
                                                        isHidden ? 'opacity-50 grayscale' : 'hover:bg-white/5'
                                                    }`}
                                                    // bare: высота строки 41px — точно как строка списка
                                                    // активов в поиске ОИ (.instrument-item, замер 41px).
                                                    style={{
                                                        ...(bare ? { height: 41, borderTop: SOFT_BORDER } : {}),
                                                        ...(isLocked ? { opacity: 0.45, filter: 'grayscale(0.5)' } : {}),
                                                    }}
                                                    title={isLocked ? 'Доступно на повышенном тарифе' : undefined}
                                                >
                                                    <td className={`${bare ? 'pl-2' : 'pl-4'} pr-0 py-1`}>
                                                        {isLocked ? (
                                                            <div
                                                                className="flex items-center justify-center w-5 h-5 rounded-lg cursor-pointer"
                                                                onClick={handleLockedClick}
                                                            >
                                                                <Lock size={14} strokeWidth={2.2} style={{ color: 'var(--text-muted)' }} />
                                                            </div>
                                                        ) : (
                                                            <div
                                                                className="flex items-center justify-center w-5 h-5 rounded-lg hover:bg-white/5 cursor-pointer transition-colors"
                                                                onClick={() => onToggleFundVisibility(fund.fund_id)}
                                                            >
                                                                {bare ? (
                                                                    <CheckBox checked={!isHidden} />
                                                                ) : (
                                                                <input
                                                                    type="checkbox"
                                                                    checked={!isHidden}
                                                                    onChange={() => {}}
                                                                    className="w-3 h-3 rounded border-theme cursor-pointer"
                                                                    style={{ accentColor: 'var(--accent)' }}
                                                                />
                                                                )}
                                                            </div>
                                                        )}
                                                    </td>
                                                    <td
                                                        className={`${bare ? 'pl-1 pr-2 overflow-hidden' : 'pl-1 pr-4'} py-1 cursor-pointer`}
                                                        onClick={isLocked ? handleLockedClick : () => onOpenFundCard(fund)}
                                                    >
                                                        <div className="flex items-center gap-2 min-w-0">
                                                            {(() => {
                                                                const uk = resolveFundLogo(fund.ticker, fund.uk_id);
                                                                if (uk) {
                                                                    return (
                                                                        <div className={`${bare ? '' : 'w-5 h-5'} rounded-full flex items-center justify-center flex-shrink-0 font-black text-sm overflow-hidden`}
                                                                            style={{
                                                                                backgroundColor: uk.img ? undefined : uk.bg,
                                                                                color: uk.color,
                                                                                // 28px в пикселях, а не w-7: root font-size 20px → w-7 = 35px.
                                                                                ...(bare ? { width: 28, height: 28 } : {}),
                                                                            }}>
                                                                            {uk.img
                                                                                ? <img src={uk.img} alt={uk.name} className="w-full h-full object-cover" />
                                                                                : uk.letter}
                                                                        </div>
                                                                    );
                                                                }
                                                                return (
                                                                    <div className="legend-dot"
                                                                        style={{ backgroundColor: FUND_COLORS[colorIdx % FUND_COLORS.length] }} />
                                                                );
                                                            })()}
                                                            <span className={`${bare ? 'font-normal' : 'font-medium'} inline-flex items-center min-w-0`} style={{ gap: 'var(--sp-1)' }}>
                                                                <span title={fund.name} className={bare ? 'truncate' : undefined} style={bare ? { minWidth: 0, maxWidth: 220 } : undefined}>{stripUkName(fund.name, fund.uk_id)}</span>
                                                                {!isLocked && lastData?.date && maxDate && lastData.date < maxDate && (
                                                                    <span
                                                                        className="text-theme-secondary cursor-help inline-flex flex-shrink-0"
                                                                        style={{ opacity: 0.6 }}
                                                                        title={`СЧА этого фонда отстаёт от общей даты данных. Актуальна на ${fmtDate(lastData.date)}`}
                                                                    >
                                                                        <AlertCircle size={17} strokeWidth={2.2} />
                                                                    </span>
                                                                )}
                                                            </span>
                                                        </div>
                                                    </td>
                                                    <td
                                                        className={`${bare ? 'px-2 text-right whitespace-nowrap' : 'px-4'} py-1 text-theme-secondary font-mono cursor-pointer`}
                                                        style={bare ? { fontSize: 'var(--fs-xs)' } : undefined}
                                                        onClick={isLocked ? handleLockedClick : () => onOpenFundCard(fund)}
                                                    >
                                                        {fund.ticker}
                                                    </td>
                                                    <td className={`${bare ? 'px-2' : 'px-4 font-mono'} py-1 text-right`} style={bare ? OI_NUM_SCHA_STYLE : undefined}>
                                                        {isLocked ? '—' : (lastData?.nav ? (lastData.nav / 1e9).toFixed(2) : '—')}
                                                    </td>
                                                    {(() => {
                                                        const br = isLocked ? null : bestReturn(fund.returns);
                                                        return (
                                                            <td className={`${bare ? 'px-2' : 'px-4 font-mono'} py-1 text-right whitespace-nowrap`} style={{
                                                                color: br ? (br.v >= 0 ? 'var(--funds-flow-positive)' : 'var(--funds-flow-negative)') : undefined,
                                                                ...(bare ? OI_NUM_STYLE : {}),
                                                            }}>
                                                                {isLocked || !br ? '—' : (
                                                                    <>
                                                                        {br.v >= 0 ? '+' : ''}{br.v.toFixed(1)}%
                                                                        {br.label !== '1г' && (
                                                                            <span className="text-theme-secondary" style={{ fontSize: 'var(--fs-2xs)', marginLeft: 3 }}>{br.label}</span>
                                                                        )}
                                                                    </>
                                                                )}
                                                            </td>
                                                        );
                                                    })()}
                                                </tr>
                                            );
                                        })}
                                    </React.Fragment>
                                );
                            });
                        })()}
                    </tbody>
                </table>
            </div>
        </div>
    );
}
