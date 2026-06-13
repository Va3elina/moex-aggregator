/**
 * FavoritePeriodSelect — селектор периода в стиле TradingView.
 *
 * Избранные периоды (со звёздочкой) показаны горизонтальным рядом сегментов
 * (переиспользуем SegmentedControl). Стрелка справа раскрывает поповер со ВСЕМ
 * списком периодов: клик по строке выбирает период, клик по звезде —
 * добавляет/убирает из избранного. Текущее значение всегда видно в ряду, даже
 * если оно не в избранном (показываем его дополнительно).
 *
 * Тариф: locked-период рисует замочек, клик уходит в onLockedClick (как в
 * Dropdown). Избранное живёт у родителя (persist в localStorage).
 *
 * Тестируется на индикаторе OI; компонент generic — переносим на другие
 * индикаторы по тому же контракту.
 */
import { useEffect, useRef, useState } from 'react';
import { ChevronDown, Star, Lock } from 'lucide-react';
import SegmentedControl from './SegmentedControl';
import { type DropdownOption } from './Dropdown';

interface FavoritePeriodSelectProps<T extends string> {
  /** ВСЕ опции в каноническом порядке (для поповера и порядка ряда). */
  options: DropdownOption<T>[];
  value: T;
  /** Ключи избранного — подмножество options, показываются в ряду. */
  favorites: T[];
  onChange: (key: T) => void;
  onToggleFavorite: (key: T) => void;
  /** Клик по locked-опции (тарифный гейт) — UpgradeModal / редирект на /login. */
  onLockedClick?: (key: T) => void;
  className?: string;
}

export default function FavoritePeriodSelect<T extends string>({
  options,
  value,
  favorites,
  onChange,
  onToggleFavorite,
  onLockedClick,
  className = '',
}: FavoritePeriodSelectProps<T>) {
  const [open, setOpen] = useState(false);
  const wrapRef = useRef<HTMLDivElement>(null);

  // Закрытие по клику вне + ESC (как в Dropdown).
  useEffect(() => {
    if (!open) return;
    const onClick = (e: MouseEvent) => {
      if (wrapRef.current && !wrapRef.current.contains(e.target as Node)) setOpen(false);
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setOpen(false);
    };
    document.addEventListener('mousedown', onClick);
    document.addEventListener('keydown', onKey);
    return () => {
      document.removeEventListener('mousedown', onClick);
      document.removeEventListener('keydown', onKey);
    };
  }, [open]);

  const favSet = new Set<T>(favorites);
  // Ряд = избранные + текущее значение (если не в избранном), в каноническом
  // порядке. Так активный период всегда виден. Если совсем пусто — показываем
  // хотя бы текущее.
  const rowOptions = options.filter((o) => favSet.has(o.key) || o.key === value);
  const shown = rowOptions.length > 0 ? rowOptions : options.filter((o) => o.key === value);

  return (
    <div
      ref={wrapRef}
      className={`relative inline-flex items-center ${className}`}
      style={{ gap: 'var(--sp-2)' }}
    >
      {shown.length > 0 && (
        <SegmentedControl<T>
          options={shown.map((o) => ({ key: o.key, label: o.label, locked: o.locked }))}
          value={value}
          onChange={onChange}
          onLockedClick={onLockedClick}
        />
      )}

      {/* Стрелка — раскрыть полный список периодов */}
      <button
        type="button"
        aria-label="Все периоды"
        aria-expanded={open}
        onClick={() => setOpen((o) => !o)}
        className="frame-dropdown-trigger inline-flex items-center justify-center font-semibold rounded-full"
        style={{
          backgroundColor: 'var(--bg-secondary)',
          color: 'var(--text-primary)',
          border: '2px solid var(--text-primary)',
          fontSize: 'var(--fs-sm)',
          padding: 'var(--sp-2) var(--sp-3)',
          cursor: 'pointer',
          transition: 'transform 0.15s ease, box-shadow 0.15s ease',
        }}
      >
        <ChevronDown
          size={16}
          style={{
            transition: 'transform 0.2s ease',
            transform: open ? 'rotate(180deg)' : 'rotate(0)',
          }}
        />
      </button>

      {open && (
        <div
          className="frame-dropdown-menu absolute z-50 py-1 rounded-xl"
          style={{
            top: 'calc(100% + 4px)',
            right: 0,
            backgroundColor: 'var(--bg-secondary)',
            border: '2px solid var(--text-primary)',
            boxShadow: 'var(--shadow-hard-chip, 4px 4px 0 var(--text-primary))',
            minWidth: 220,
            maxHeight: '60vh',
            overflowY: 'auto',
          }}
        >
          {options.map((opt) => {
            const active = opt.key === value;
            const isFav = favSet.has(opt.key);
            return (
              <div
                key={opt.key}
                className="flex items-center"
                style={{ margin: '2px 6px', gap: 'var(--sp-1)' }}
              >
                <button
                  type="button"
                  disabled={opt.locked && !onLockedClick}
                  onClick={() => {
                    if (opt.locked) {
                      onLockedClick?.(opt.key);
                      setOpen(false);
                      return;
                    }
                    onChange(opt.key);
                    setOpen(false);
                  }}
                  className="flex-1 text-left text-sm flex items-center gap-2"
                  style={{
                    padding: '8px 12px',
                    borderRadius: 999,
                    fontWeight: active ? 800 : 600,
                    color: opt.locked
                      ? 'var(--text-muted)'
                      : active
                        ? 'var(--text-inverse)'
                        : 'var(--text-primary)',
                    backgroundColor: active ? 'var(--accent)' : 'transparent',
                    border: active ? '2px solid var(--text-primary)' : '2px solid transparent',
                    boxShadow: active ? 'var(--shadow-hard-chip, 3px 3px 0 var(--text-primary))' : 'none',
                    cursor: opt.locked ? 'not-allowed' : 'pointer',
                    opacity: opt.locked ? 0.6 : 1,
                    whiteSpace: 'nowrap',
                  }}
                >
                  <span className="flex-1">{opt.label}</span>
                  {opt.locked && <Lock size={12} className="flex-shrink-0" />}
                </button>
                {/* Звезда — добавить/убрать из избранного (ряд обновляется сразу) */}
                <button
                  type="button"
                  aria-label={isFav ? 'Убрать из избранного' : 'Добавить в избранное'}
                  aria-pressed={isFav}
                  onClick={() => onToggleFavorite(opt.key)}
                  className="inline-flex items-center justify-center rounded-full flex-shrink-0"
                  style={{
                    padding: 6,
                    cursor: 'pointer',
                    color: isFav ? 'var(--accent)' : 'var(--text-muted)',
                  }}
                >
                  <Star size={16} fill={isFav ? 'currentColor' : 'none'} strokeWidth={2} />
                </button>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
