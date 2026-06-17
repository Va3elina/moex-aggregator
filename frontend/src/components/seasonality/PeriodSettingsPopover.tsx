/**
 * PeriodSettingsPopover — шестерёнка на чипе «Период с YYYY» + якорный попап с
 * настройками этой серии: «Без выбросов» (медиана) и «Без дивидендных гэпов».
 *
 * Раньше эти опции были глобальными кнопками в горячем ряду и не комбинировались
 * с периодом (медиана считалась по всей истории отдельной серией). Теперь они
 * живут у каждого периода и применяются именно к нему.
 *
 * Паттерн open/outside-click/ESC переиспользован из Dropdown.tsx. Дивидендный
 * тоггл скрыт для инструментов без дивидендов (индексы/валюты/сырьё).
 */
import { useEffect, useRef, useState } from 'react';
import { Settings2 } from 'lucide-react';
import { ToggleRow } from '../ToggleRow';
import type { PeriodConfig } from './periodConfig';

interface PeriodSettingsPopoverProps {
  period: PeriodConfig;
  hasDividends: boolean;
  onChange: (patch: Partial<Pick<PeriodConfig, 'median' | 'excludeDividends'>>) => void;
}

export default function PeriodSettingsPopover({ period, hasDividends, onChange }: PeriodSettingsPopoverProps) {
  const [open, setOpen] = useState(false);
  const wrapRef = useRef<HTMLDivElement>(null);

  // Закрытие по клику вне + ESC (как в Dropdown).
  useEffect(() => {
    if (!open) return;
    const onClick = (e: MouseEvent) => {
      if (wrapRef.current && !wrapRef.current.contains(e.target as Node)) setOpen(false);
    };
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') setOpen(false); };
    document.addEventListener('mousedown', onClick);
    document.addEventListener('keydown', onKey);
    return () => {
      document.removeEventListener('mousedown', onClick);
      document.removeEventListener('keydown', onKey);
    };
  }, [open]);

  // Активная настройка подсвечивает шестерёнку — видно, что серия модифицирована,
  // не открывая попап.
  const active = period.median || period.excludeDividends;

  return (
    <div ref={wrapRef} className="relative inline-flex" data-export-ignore="true">
      <button
        type="button"
        onClick={() => setOpen(o => !o)}
        className="inline-flex items-center justify-center rounded-full transition-opacity hover:opacity-100"
        style={{ opacity: active ? 1 : 0.6, color: active ? 'var(--accent)' : 'var(--text-primary)' }}
        title="Настройки периода: без выбросов / без дивидендных гэпов"
        aria-label="Настройки периода"
      >
        <Settings2 size={14} />
      </button>
      {open && (
        <div
          className="absolute z-50 mt-1 p-2 rounded-xl"
          style={{
            top: '100%',
            left: 0,
            minWidth: 220,
            backgroundColor: 'var(--bg-secondary)',
            border: '2px solid var(--text-primary)',
            boxShadow: 'var(--shadow-hard-chip, 4px 4px 0 var(--text-primary))',
            display: 'flex',
            flexDirection: 'column',
            gap: 'var(--sp-2)',
          }}
        >
          <ToggleRow
            label="Без выбросов"
            hint="Медиана вместо среднего — устойчива к кризисным годам"
            checked={period.median}
            onChange={(v) => onChange({ median: v })}
          />
          {hasDividends && (
            <ToggleRow
              label="Без дивидендных гэпов"
              hint="Цены с реинвестированием дивидендов (adjusted close)"
              checked={period.excludeDividends}
              onChange={(v) => onChange({ excludeDividends: v })}
            />
          )}
        </div>
      )}
    </div>
  );
}
