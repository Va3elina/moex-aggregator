/**
 * FundPickerModal — окно «Фонды <категория>»: список фондов с галочками, выбор
 * применяется сразу по клику, «Готово» лишь закрывает.
 *
 * Один компонент на два места: страницу /funds-money и виджет потоков фондов в
 * песочнице/расширении. Раньше разметка жила инлайном в FundsMoneyPage, а в
 * embed'е стоял свой чек-лист в поповере тулбара — два разных UI для одного
 * действия.
 *
 * ⚠️ Рендерится ПОРТАЛОМ в body. В песочнице панель — предок с backdrop-filter,
 * а он создаёт containing block для position: fixed: без портала окно
 * позиционируется относительно панели и обрезается её границами (тот же капкан,
 * что с ColorPicker и HelpTooltip, #956).
 *
 * collapsedSubcats / navSortDir можно не передавать — тогда живут внутри.
 * Страница передаёт свои, чтобы состояние списка переживало закрытие окна.
 */
import React, { useMemo, useState } from 'react';
import { createPortal } from 'react-dom';
import { AlertCircle, X } from 'lucide-react';
import FundsTable from './FundsTable';
import type { FundsChartResponse } from '../../services/api';

interface Props {
  data: FundsChartResponse | null;
  hiddenFunds: Set<number>;
  onSetHiddenFunds: React.Dispatch<React.SetStateAction<Set<number>>>;
  onToggleFundVisibility: (fundId: number) => void;
  onClose: () => void;
  /** Родительный падеж категории для заголовка: «Фонды облигаций». */
  categoryGenitive?: string;
  /** Дата среза данных (ISO) и флаг «часть фондов запаздывает» — подпись в шапке. */
  maxDate?: string;
  hasStale?: boolean;
  /** Итог по видимым фондам для шапки таблицы. Не передан — считаем сами. */
  aggregatedData?: { chartData: { time: string; value: number }[]; totalCurrentNav: number };
  collapsedSubcats?: Set<string>;
  onSetCollapsedSubcats?: React.Dispatch<React.SetStateAction<Set<string>>>;
  navSortDir?: 'desc' | 'asc';
  onSetNavSortDir?: React.Dispatch<React.SetStateAction<'desc' | 'asc'>>;
}

const fmtDate = (iso: string) => iso.split('-').reverse().join('.');

export default function FundPickerModal({
  data,
  hiddenFunds,
  onSetHiddenFunds,
  onToggleFundVisibility,
  onClose,
  categoryGenitive,
  maxDate,
  hasStale,
  aggregatedData,
  collapsedSubcats,
  onSetCollapsedSubcats,
  navSortDir,
  onSetNavSortDir,
}: Props) {
  const [ownCollapsed, setOwnCollapsed] = useState<Set<string>>(new Set());
  const [ownSortDir, setOwnSortDir] = useState<'desc' | 'asc'>('desc');

  // Итог по видимым фондам: FundsTable показывает его в шапке. Странице он уже
  // посчитан для графика, embed'у считать отдельно незачем — берём последний
  // известный nav каждого видимого фонда.
  const ownAggregated = useMemo(() => {
    const visible = (data?.funds ?? []).filter((f) => !hiddenFunds.has(f.fund_id));
    let total = 0;
    for (const f of visible) {
      for (let i = f.data.length - 1; i >= 0; i--) {
        const nav = f.data[i]?.nav;
        if (nav != null) { total += nav; break; }
      }
    }
    return { chartData: [], totalCurrentNav: total };
  }, [data, hiddenFunds]);

  // Счётчик для «Готово»: считаем по ВСЕМ строкам списка (включая locked — они
  // тоже с чекбоксом), чтобы цифра совпадала с тем, что видно в окне.
  const selectedCount = useMemo(
    () => (data?.funds ?? []).filter((f) => !hiddenFunds.has(f.fund_id)).length,
    [data, hiddenFunds],
  );
  const allSelected = selectedCount === (data?.funds.length ?? 0);

  return createPortal(
    // Масштаб/шелл как у InstrumentSearchModal на ОИ: top-anchored, max-h-90vh,
    // 2px border + hard shadow, скролл внутри.
    <div className="fixed inset-0 z-[100000] flex items-start justify-center p-4 pt-8 sm:pt-10">
      <div
        className="absolute inset-0"
        style={{ backgroundColor: 'rgba(0,0,0,0.6)' }}
        onClick={onClose}
      />
      <div
        className="relative w-full max-w-xl rounded-2xl max-h-[90vh] overflow-hidden flex flex-col"
        style={{
          backgroundColor: 'var(--bg-secondary)',
          border: '2px solid var(--text-primary)',
          boxShadow: 'var(--shadow-hard-chip, 6px 6px 0 var(--text-primary))',
          color: 'var(--text-primary)',
          // max-w-xl (576px) не хватало колонке «Название» у длинных имён
          // облигационных фондов («Фонд Долгосрочные гособлигации» и т.п.) —
          // они фейдились почти целиком. Ширина 816 = прежние 680 +20%;
          // лишние px уходят авто-колонке Название (Тикер/СЧА/Доходность —
          // фиксированные px в colgroup).
          maxWidth: 816,
        }}
      >
        {/* Заголовок + дата данных в ОДНОМ ряду, справа — крестик. */}
        <div
          className="flex items-center flex-shrink-0 flex-wrap"
          style={{ padding: 'var(--sp-4) var(--sp-5) var(--sp-3)', gap: '4px 12px', borderBottom: '1px solid color-mix(in srgb, var(--text-primary) 12%, transparent)' }}
        >
          <span className="font-semibold" style={{ fontSize: 'var(--fs-base)' }}>
            Фонды {categoryGenitive ?? ''}
          </span>
          {maxDate && (
            <span className="inline-flex items-center" style={{ gap: 8, fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)' }}>
              <span>На {fmtDate(maxDate)}</span>
              {hasStale && (
                <>
                  <span style={{ opacity: 0.45 }}>•</span>
                  <span className="inline-flex items-center" style={{ gap: 5 }}>
                    <AlertCircle size={13} strokeWidth={2.2} style={{ opacity: 0.6, flexShrink: 0 }} />
                    часть фондов запаздывает
                  </span>
                </>
              )}
            </span>
          )}
          <button
            onClick={onClose}
            className="p-2 -mr-2 rounded-lg transition-colors flex-shrink-0 ml-auto"
            style={{ color: 'var(--text-secondary)' }}
            aria-label="Закрыть"
          >
            <X size={22} />
          </button>
        </div>
        {/* Симметричные боковые отступы: padding одинаков слева/справа, а
            scrollbar-gutter both-edges резервирует место скроллбара С ОБЕИХ
            сторон — иначе вертикальный скроллбар (~11px справа) съедал правый
            отступ и строки стояли несимметрично. */}
        <div className="flex-1 min-h-0 overflow-y-auto styled-scrollbar" style={{ padding: '0 var(--sp-4) var(--sp-4)', scrollbarGutter: 'stable both-edges' }}>
          <FundsTable
            bare
            data={data}
            hiddenFunds={hiddenFunds}
            collapsedSubcats={collapsedSubcats ?? ownCollapsed}
            navSortDir={navSortDir ?? ownSortDir}
            aggregatedData={aggregatedData ?? ownAggregated}
            onToggleFundVisibility={onToggleFundVisibility}
            onSetHiddenFunds={onSetHiddenFunds}
            onSetCollapsedSubcats={onSetCollapsedSubcats ?? setOwnCollapsed}
            onSetNavSortDir={onSetNavSortDir ?? setOwnSortDir}
          />
        </div>
        {/* «Готово» — как в пикере фондов «Сделок фондов»: выбор применяется
            сразу по клику, кнопка лишь закрывает окно, но даёт очевидный выход
            и счётчик выбранных. */}
        <div
          className="px-6 py-4 flex-shrink-0"
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
            Готово{allSelected ? '' : ` · ${selectedCount}`}
          </button>
        </div>
      </div>
    </div>,
    document.body,
  );
}
