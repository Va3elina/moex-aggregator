/**
 * SegmentedControl — плитки-сегменты в editorial pill-стиле для desktop-тулбаров.
 *
 * Замена Dropdown'у там, где значение переключают часто (таймфрейм 5м/1ч/1д):
 * выбор в один клик без открытия меню, активный сегмент виден сразу.
 * Визуально — один pill с внутренними перегородками (рамка как у
 * Dropdown-trigger), активный сегмент = accent + inverse text.
 *
 * locked-опции: видимы с замочком, клик уходит в onLockedClick (UpgradeModal /
 * login-gate) — паритет с DropdownOption.locked. Несуществующие у актива
 * значения сюда не передавать вовсе (фильтровать на странице, как для Dropdown).
 */
import { Lock } from 'lucide-react';

export interface SegmentOption<T extends string> {
  key: T;
  label: string;
  /** Не кликабелен по тарифу/гостевому гейту — рисуем замочек */
  locked?: boolean;
  /** title-подсказка на сегменте (полное имя для короткого лейбла) */
  title?: string;
}

interface SegmentedControlProps<T extends string> {
  options: SegmentOption<T>[];
  value: T;
  onChange: (key: T) => void;
  /** Клик по locked-сегменту (показ UpgradeModal / редирект на /login) */
  onLockedClick?: (key: T) => void;
  className?: string;
}

export default function SegmentedControl<T extends string>({
  options,
  value,
  onChange,
  onLockedClick,
  className = '',
}: SegmentedControlProps<T>) {
  return (
    <div
      role="group"
      className={`frame-segmented rounded-full overflow-hidden ${className}`}
      style={{
        backgroundColor: 'var(--bg-secondary)',
        border: '2px solid var(--text-primary)',
        // Равные колонки (по самому широкому лейблу), иначе «5м» шире «1д»
        // и контрол выглядит несимметрично.
        display: 'inline-grid',
        gridAutoFlow: 'column',
        gridAutoColumns: '1fr',
      }}
    >
      {options.map((opt, i) => {
        const active = opt.key === value;
        return (
          <button
            key={opt.key}
            type="button"
            title={opt.title}
            aria-pressed={active}
            onClick={() => {
              if (opt.locked) { onLockedClick?.(opt.key); return; }
              if (!active) onChange(opt.key);
            }}
            className="frame-segmented-item font-semibold inline-flex items-center justify-center"
            style={{
              fontSize: 'var(--fs-sm)',
              padding: 'var(--sp-2) var(--sp-3)',
              gap: 4,
              borderLeft: i > 0 ? '2px solid var(--text-primary)' : 'none',
              backgroundColor: active ? 'var(--accent)' : 'transparent',
              color: opt.locked
                ? 'var(--text-muted)'
                : active
                  ? 'var(--text-inverse)'
                  : 'var(--text-primary)',
              opacity: opt.locked ? 0.55 : 1,
              cursor: opt.locked ? 'not-allowed' : 'pointer',
              whiteSpace: 'nowrap',
              transition: 'background-color 0.12s ease, color 0.12s ease',
            }}
          >
            {opt.label}
            {opt.locked && <Lock size={11} className="flex-shrink-0" />}
          </button>
        );
      })}
    </div>
  );
}
