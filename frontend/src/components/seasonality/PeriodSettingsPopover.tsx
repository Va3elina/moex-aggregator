/**
 * PeriodSettingsPopover — чип «Период с YYYY» целиком + попап настроек серии:
 * «Без выбросов» (медиана) и «Без дивидендных гэпов».
 *
 * Триггер — ВЕСЬ чип: клик в любое место открывает настройки (раньше нужно было
 * целиться в маленькую шестерёнку). Шестерёнка осталась как визуальный hint и
 * подсвечивается accent'ом, когда серия модифицирована. Крестик удаления гасит
 * всплытие, чтобы не открывать настройки при удалении.
 *
 * Попап рендерится через ПОРТАЛ в document.body, а не absolute внутри чипа.
 * Причина: чип имеет класс `editorial-press`, который на :hover/:active ставит
 * `transform: translate(...)` (см. index.css). Любой transform создаёт новый
 * stacking context, и z-index absolute-попапа оказывается заперт внутри чипа —
 * карточка графика (ниже по DOM) рисовалась поверх попапа. Портал в body выносит
 * попап из этого контекста; позиционируем его fixed по координатам чипа.
 *
 * Дивидендный тоггл скрыт для инструментов без дивидендов (индексы/валюты/сырьё).
 */
import { useCallback, useEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { Settings2, X } from 'lucide-react';
import { ToggleRow } from '../ToggleRow';
import type { PeriodConfig } from './periodConfig';

interface PeriodSettingsPopoverProps {
  period: PeriodConfig;
  /** Цвет точки серии (синхронизирован с линией на графике). */
  color: string;
  /** Можно ли убрать период (false для единственного активного). */
  removable: boolean;
  onRemove: () => void;
  hasDividends: boolean;
  onChange: (patch: Partial<Pick<PeriodConfig, 'median' | 'excludeDividends'>>) => void;
  title?: string;
}

const POPOVER_WIDTH = 240;

export default function PeriodSettingsPopover({
  period, color, removable, onRemove, hasDividends, onChange, title,
}: PeriodSettingsPopoverProps) {
  const [open, setOpen] = useState(false);
  const chipRef = useRef<HTMLButtonElement>(null);
  const popRef = useRef<HTMLDivElement>(null);
  const [pos, setPos] = useState<{ top: number; left: number } | null>(null);

  // Позиция попапа: строго ВНИЗ из-под чипа (fixed → относительно viewport,
  // кламп по правому краю окна).
  const place = useCallback(() => {
    const anchor = chipRef.current;
    if (!anchor) return;
    const r = anchor.getBoundingClientRect();
    const left = Math.min(r.left, window.innerWidth - POPOVER_WIDTH - 8);
    setPos({ top: r.bottom + 6, left: Math.max(8, left) });
  }, []);

  useEffect(() => {
    if (!open) return;
    const onPointer = (e: MouseEvent) => {
      const t = e.target as Node;
      if (chipRef.current?.contains(t) || popRef.current?.contains(t)) return;
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
  const mods = [period.median && 'медиана', period.excludeDividends && 'без див.']
    .filter(Boolean).join(', ');

  return (
    <>
      <button
        ref={chipRef}
        type="button"
        onClick={() => { if (!open) place(); setOpen((o) => !o); }}
        title={title}
        aria-haspopup="dialog"
        aria-expanded={open}
        className="editorial-press flex items-center font-semibold rounded-full whitespace-nowrap"
        style={{
          background: 'var(--bg-secondary)',
          color: 'var(--text-primary)',
          border: '2px solid var(--text-primary)',
          fontSize: 'var(--fs-sm)',
          padding: 'var(--sp-2) var(--sp-3)',
          gap: 'var(--sp-2)',
          cursor: 'pointer',
        }}
      >
        <span
          className="inline-block rounded-full"
          style={{ width: 'var(--ico-xs)', height: 'var(--ico-xs)', backgroundColor: color, flexShrink: 0 }}
        />
        С {period.sinceYear} г.
        {mods && (
          <span style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-muted)' }}>({mods})</span>
        )}
        <Settings2
          size={16}
          data-export-ignore="true"
          style={{ flexShrink: 0, color: active ? 'var(--accent)' : 'var(--text-muted)' }}
        />
        {removable && (
          <span
            role="button"
            tabIndex={0}
            data-export-ignore="true"
            onClick={(e) => { e.stopPropagation(); onRemove(); }}
            onKeyDown={(e) => {
              if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); e.stopPropagation(); onRemove(); }
            }}
            className="opacity-60 hover:opacity-100 transition-opacity inline-flex items-center"
            title="Убрать период"
            aria-label="Убрать период"
            style={{ cursor: 'pointer', flexShrink: 0 }}
          >
            <X size={14} />
          </span>
        )}
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
            hint="Считает по медиане, а не по среднему. Кризисные годы не перетягивают значение, видно типичную сезонность."
            checked={period.median}
            onChange={(v) => onChange({ median: v })}
          />
          {hasDividends && (
            <ToggleRow
              label="Без дивидендных гэпов"
              hint="Цены пересчитаны с учётом дивидендов. Провалы в дни отсечки не считаются падением и не искажают сезонность."
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
