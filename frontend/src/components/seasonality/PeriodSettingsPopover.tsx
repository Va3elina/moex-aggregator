/**
 * PeriodSettingsPopover — шестерёнка на чипе «Период с YYYY» + попап с настройками
 * этой серии: «Без выбросов» (медиана) и «Без дивидендных гэпов».
 *
 * Раньше эти опции были глобальными кнопками и не комбинировались с периодом
 * (медиана считалась по всей истории отдельной серией). Теперь они живут у каждого
 * периода и применяются именно к нему.
 *
 * Попап рендерится через ПОРТАЛ в document.body, а не absolute внутри чипа.
 * Причина: чип имеет класс `editorial-press`, который на :hover/:active ставит
 * `transform: translate(...)` (см. index.css). Любой transform создаёт новый
 * stacking context, и z-index absolute-попапа оказывается заперт внутри чипа —
 * карточка графика (ниже по DOM) рисовалась поверх попапа. Портал в body выносит
 * попап из этого контекста; позиционируем его fixed по координатам шестерёнки.
 *
 * Дивидендный тоггл скрыт для инструментов без дивидендов (индексы/валюты/сырьё).
 */
import { useCallback, useEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { Settings2 } from 'lucide-react';
import { ToggleRow } from '../ToggleRow';
import type { PeriodConfig } from './periodConfig';

interface PeriodSettingsPopoverProps {
  period: PeriodConfig;
  hasDividends: boolean;
  onChange: (patch: Partial<Pick<PeriodConfig, 'median' | 'excludeDividends'>>) => void;
}

const POPOVER_WIDTH = 240;

export default function PeriodSettingsPopover({ period, hasDividends, onChange }: PeriodSettingsPopoverProps) {
  const [open, setOpen] = useState(false);
  const btnRef = useRef<HTMLButtonElement>(null);
  const popRef = useRef<HTMLDivElement>(null);
  const [pos, setPos] = useState<{ top: number; left: number } | null>(null);

  // Позиция попапа: выкатывается строго ВНИЗ из-под чипа «Период с» (а не из-под
  // шестерёнки справа — иначе казалось, что он уезжает по диагонали вправо).
  // Якорь — сам чип (.editorial-press), fixed → относительно viewport, кламп по
  // правому краю окна.
  const place = useCallback(() => {
    const gear = btnRef.current;
    if (!gear) return;
    const anchor = gear.closest('.editorial-press') ?? gear;
    const r = anchor.getBoundingClientRect();
    const left = Math.min(r.left, window.innerWidth - POPOVER_WIDTH - 8);
    setPos({ top: r.bottom + 6, left: Math.max(8, left) });
  }, []);

  useEffect(() => {
    if (!open) return;
    const onPointer = (e: MouseEvent) => {
      const t = e.target as Node;
      if (btnRef.current?.contains(t) || popRef.current?.contains(t)) return;
      setOpen(false);
    };
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') setOpen(false); };
    document.addEventListener('mousedown', onPointer);
    document.addEventListener('keydown', onKey);
    // Попап fixed — при скролле/ресайзе пересчитываем позицию (или закрылись бы «висеть»).
    window.addEventListener('scroll', place, true);
    window.addEventListener('resize', place);
    return () => {
      document.removeEventListener('mousedown', onPointer);
      document.removeEventListener('keydown', onKey);
      window.removeEventListener('scroll', place, true);
      window.removeEventListener('resize', place);
    };
  }, [open, place]);

  // Активная настройка подсвечивает шестерёнку — видно, что серия модифицирована.
  const active = period.median || period.excludeDividends;

  return (
    <>
      <button
        ref={btnRef}
        type="button"
        data-export-ignore="true"
        onClick={() => { if (!open) place(); setOpen((o) => !o); }}
        className="inline-flex items-center justify-center rounded-full transition-opacity hover:opacity-100"
        style={{ opacity: active ? 1 : 0.6, color: active ? 'var(--accent)' : 'var(--text-primary)' }}
        title="Настройки периода: без выбросов / без дивидендных гэпов"
        aria-label="Настройки периода"
      >
        <Settings2 size={14} />
      </button>
      {open && pos && createPortal(
        <div
          ref={popRef}
          data-export-ignore="true"
          style={{
            position: 'fixed',
            top: pos.top,
            left: pos.left,
            width: POPOVER_WIDTH,
            zIndex: 1000,
            padding: 'var(--sp-2)',
            borderRadius: 12,
            backgroundColor: 'var(--bg-secondary)',
            border: '2px solid var(--text-primary)',
            boxShadow: 'var(--shadow-hard-chip, 4px 4px 0 var(--text-primary))',
            display: 'flex',
            flexDirection: 'column',
            gap: 'var(--sp-2)',
            transformOrigin: 'top left',
            animation: 'period-popover-in 0.14s cubic-bezier(0.22, 1, 0.36, 1)',
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
        </div>,
        document.body,
      )}
    </>
  );
}
