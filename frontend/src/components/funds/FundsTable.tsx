import React from 'react';
import { Lock } from 'lucide-react';
import { UK_LOGOS } from '../../config/fundConfig';
import type { FundInfo, FundsChartResponse } from '../../services/api';
import { useUpgradePrompt } from '../tier/UpgradeModal';
import { useTierAccess } from '../../contexts/TierFeaturesContext';

const FUND_COLORS = [
    '#2EE59D', '#4DA3FF', '#9D4DFF', '#FF4D4D', '#FFB020',
    '#00D9FF', '#FF6B9D', '#FCD34D', '#14B8A6', '#F97316'
];

// Подкатегории, у которых данные (NAV) ещё наливаются — показываем бейдж
// «Скоро» на заголовке группы. Убрать имя отсюда, когда NAV появится.
const COMING_SOON_SUBCATS = new Set<string>(['Блогеры']);

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
}: FundsTableProps) {
    const { showUpgrade } = useUpgradePrompt();
    const fundsAccess = useTierAccess('funds_money');

    return (
        <div className="mt-6 rounded-2xl overflow-hidden editorial-frame" style={{ background: 'var(--bg-secondary)', padding: 0 }}>
            <div className="border-b border-theme flex items-center justify-between" style={{ padding: 'var(--sp-3) var(--sp-4)' }}>
                <h3 className="font-semibold" style={{ fontSize: 'var(--fs-base)' }}>Фонды категории</h3>
                <div className="flex items-center" style={{ gap: 'var(--sp-2)' }}>
                    <span className="text-theme-secondary" style={{ fontSize: 'var(--fs-sm)' }}>Суммарная СЧА выбранных:</span>
                    <span className="font-mono font-bold" style={{ color: 'var(--funds-flow-positive)', fontSize: 'var(--fs-sm)' }}>
                        {aggregatedData.totalCurrentNav.toFixed(2)} млрд ₽
                    </span>
                </div>
            </div>
            <div className="overflow-x-auto">
                <table className="w-full" style={{ fontSize: 'var(--fs-sm)' }}>
                    <thead>
                        <tr className="text-theme-secondary text-left">
                            <th className="px-4 py-3 font-medium w-10"></th>
                            <th className="px-4 py-3 font-medium">Тикер</th>
                            <th className="px-4 py-3 font-medium">Название</th>
                            <th className="px-4 py-3 font-medium text-right">
                                <button
                                    onClick={() => onSetNavSortDir(d => d === 'desc' ? 'asc' : 'desc')}
                                    className="inline-flex items-center gap-1 hover:text-theme-primary transition-colors"
                                >
                                    СЧА
                                    <span className="text-xs">{navSortDir === 'desc' ? '↓' : '↑'}</span>
                                </button>
                            </th>
                            <th className="px-4 py-3 font-medium text-right">Дата</th>
                        </tr>
                    </thead>
                    <tbody>
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
                                            <tr className="border-t-2 border-theme"
                                                style={{ background: 'color-mix(in srgb, var(--text-primary) 4%, transparent)' }}>
                                                <td className="px-4 py-3">
                                                    <div
                                                        className="flex items-center justify-center w-8 h-8 rounded-lg hover:bg-white/5 cursor-pointer transition-colors"
                                                        onClick={toggleSubcat}
                                                    >
                                                        <input
                                                            type="checkbox"
                                                            checked={!allHidden}
                                                            ref={el => { if (el) el.indeterminate = someHidden && !allHidden; }}
                                                            onChange={() => {}}
                                                            className="w-4 h-4 rounded border-theme cursor-pointer"
                                                            style={{ accentColor: 'var(--accent)' }}
                                                        />
                                                    </div>
                                                </td>
                                                <td colSpan={4} className="px-4 py-3 cursor-pointer select-none" onClick={toggleCollapse}>
                                                    <div className="flex items-center gap-2">
                                                        <span className="text-[10px] text-theme-secondary transition-transform duration-200" style={{ transform: isCollapsed ? 'rotate(-90deg)' : 'rotate(0deg)' }}>▼</span>
                                                        <span className="text-sm font-bold text-theme-primary">
                                                            {subcat}
                                                        </span>
                                                        <span className="text-xs text-theme-secondary">({groupFunds.length})</span>
                                                        {subcat && COMING_SOON_SUBCATS.has(subcat) && (
                                                            <span className="rounded-full shrink-0" style={{ fontSize: 'var(--fs-2xs)', padding: 'calc(var(--sp-1)) var(--sp-2)', background: 'var(--accent)', color: 'var(--text-inverse)', fontWeight: 700 }}>
                                                                Скоро
                                                            </span>
                                                        )}
                                                    </div>
                                                </td>
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
                                                    className={`border-t border-theme transition-colors ${
                                                        isLocked ? 'cursor-not-allowed' :
                                                        isHidden ? 'opacity-50 grayscale' : 'hover:bg-white/5'
                                                    }`}
                                                    style={isLocked ? { opacity: 0.45, filter: 'grayscale(0.5)' } : undefined}
                                                    title={isLocked ? 'Доступно на повышенном тарифе' : undefined}
                                                >
                                                    <td className="px-4 py-3">
                                                        {isLocked ? (
                                                            <div
                                                                className="flex items-center justify-center w-8 h-8 rounded-lg cursor-pointer"
                                                                onClick={handleLockedClick}
                                                            >
                                                                <Lock size={14} strokeWidth={2.2} style={{ color: 'var(--text-muted)' }} />
                                                            </div>
                                                        ) : (
                                                            <div
                                                                className="flex items-center justify-center w-8 h-8 rounded-lg hover:bg-white/5 cursor-pointer transition-colors"
                                                                onClick={() => onToggleFundVisibility(fund.fund_id)}
                                                            >
                                                                <input
                                                                    type="checkbox"
                                                                    checked={!isHidden}
                                                                    onChange={() => {}}
                                                                    className="w-4 h-4 rounded border-theme cursor-pointer"
                                                                    style={{ accentColor: 'var(--accent)' }}
                                                                />
                                                            </div>
                                                        )}
                                                    </td>
                                                    <td
                                                        className="px-4 py-3 cursor-pointer"
                                                        onClick={isLocked ? handleLockedClick : () => onOpenFundCard(fund)}
                                                    >
                                                        <div className="flex items-center gap-2">
                                                            {(() => {
                                                                const uk = fund.uk_id ? UK_LOGOS[fund.uk_id] : null;
                                                                if (uk) {
                                                                    return (
                                                                        <div className="w-7 h-7 rounded-full flex items-center justify-center flex-shrink-0 font-black text-sm"
                                                                            style={{ backgroundColor: uk.bg, color: uk.color }}>
                                                                            {uk.letter}
                                                                        </div>
                                                                    );
                                                                }
                                                                return (
                                                                    <div className="legend-dot"
                                                                        style={{ backgroundColor: FUND_COLORS[colorIdx % FUND_COLORS.length] }} />
                                                                );
                                                            })()}
                                                            <span className="font-medium">{fund.ticker}</span>
                                                        </div>
                                                    </td>
                                                    <td
                                                        className="px-4 py-3 text-theme-secondary cursor-pointer"
                                                        onClick={isLocked ? handleLockedClick : () => onOpenFundCard(fund)}
                                                    >
                                                        {fund.name}
                                                    </td>
                                                    <td className="px-4 py-3 text-right font-mono">
                                                        {isLocked ? '—' : (lastData?.nav ? `${(lastData.nav / 1e9).toFixed(2)} млрд ₽` : '—')}
                                                    </td>
                                                    <td className="px-4 py-3 text-right text-theme-secondary">
                                                        {isLocked ? '—' : (lastData?.date || '—')}
                                                    </td>
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
