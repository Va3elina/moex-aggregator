/**
 * SegmentedControl — плитки-сегменты в editorial pill-стиле для desktop-тулбаров.
 *
 * Замена Dropdown'у там, где значение переключают часто (таймфрейм 5м/1ч/1д):
 * выбор в один клик без открытия меню, активный сегмент виден сразу.
 * Визуально — один pill с внутренними перегородками (рамка как у
 * Dropdown-trigger), активный сегмент = accent + inverse text.
 *
 * Два вида недоступности (важно различать — это РАЗНЫЕ причины):
 *  - locked: данные есть, но закрыты тарифом/гостевым гейтом → серый + замочек,
 *    клик уходит в onLockedClick (UpgradeModal / login). Это «улучшай тариф».
 *  - disabled: данных в этом таймфрейме у инструмента просто НЕТ (ISS-only
 *    активы, крипта — только дневка) → серый, БЕЗ замочка, не кликается,
 *    причину объясняет тултип (title). Замочек тут вводил бы в заблуждение,
 *    будто можно «разблокировать». Так контрол всегда из 3 сегментов, без
 *    схлопывания в одну странную кнопку.
 */
import { useState } from 'react';
import type { ReactNode } from 'react';
import { Lock } from 'lucide-react';

export interface SegmentOption<T extends string> {
  key: T;
  label: string;
  /** Не кликабелен по тарифу/гостевому гейту — рисуем замочек */
  locked?: boolean;
  /** Данных нет у инструмента — серый, без замочка, не кликается (см. title) */
  disabled?: boolean;
  /** title-подсказка на сегменте (полное имя / причина недоступности) */
  title?: string;
}

interface SegmentedControlProps<T extends string> {
  options: SegmentOption<T>[];
  value: T;
  onChange: (key: T) => void;
  /** Клик по locked-сегменту (показ UpgradeModal / редирект на /login) */
  onLockedClick?: (key: T) => void;
  className?: string;
  /** Трейлинг-ячейка ВНУТРИ пилюли (обычно HelpTooltip «?»), отделена
   *  перегородкой как обычный сегмент. Привязывает подсказку к переключателю
   *  общей обводкой — «?» перестаёт «висеть» отдельным кружком между
   *  виджетами. Сам сегмент при этом некликабелен (это не режим). */
  trailing?: ReactNode;
}

export default function SegmentedControl<T extends string>({
  options,
  value,
  onChange,
  onLockedClick,
  className = '',
  trailing,
}: SegmentedControlProps<T>) {
  const [hoveredKey, setHoveredKey] = useState<T | null>(null);

  const segmentGrid = (
    <div
      {...(trailing
        ? {}
        : { role: 'group' as const, className: `frame-segmented rounded-full overflow-hidden ${className}` })}
      style={{
        // Равные колонки (по самому широкому лейблу), иначе «5м» шире «1д»
        // и контрол выглядит несимметрично.
        display: 'inline-grid',
        gridAutoFlow: 'column',
        gridAutoColumns: '1fr',
        ...(trailing
          // С trailing рамка/фон/скругление уходят на внешнюю flex-пилюлю,
          // а сетка лишь клиппит заливку и скругляет ЛЕВУЮ сторону.
          ? { overflow: 'hidden', borderRadius: '9999px 0 0 9999px' }
          // Без trailing — самостоятельная пилюля (как было исторически),
          // рамка и фон живут прямо на сетке.
          : { backgroundColor: 'var(--bg-secondary)', border: '2px solid var(--text-primary)' }),
      }}
    >
      {options.map((opt, i) => {
        const active = opt.key === value;
        const muted = opt.locked || opt.disabled;
        return (
          <button
            key={opt.key}
            type="button"
            title={opt.title}
            aria-pressed={active}
            aria-disabled={opt.disabled || undefined}
            onClick={() => {
              if (opt.disabled) return;           // данных нет — глухо
              if (opt.locked) { onLockedClick?.(opt.key); return; }
              if (!active) onChange(opt.key);
            }}
            onMouseEnter={() => setHoveredKey(opt.key)}
            onMouseLeave={() => setHoveredKey((k) => (k === opt.key ? null : k))}
            className="frame-segmented-item font-semibold inline-flex items-center justify-center"
            style={{
              fontSize: 'var(--fs-sm)',
              padding: 'var(--sp-2) var(--sp-3)',
              gap: 4,
              borderLeft: i > 0 ? '2px solid var(--text-primary)' : 'none',
              // hover «пред-нажим»: лёгкая accent-подсветка неактивного сегмента
              // (как у inactive dropdown-item), без translate — чтобы не ломать
              // единую pill. transition по background-color уже задан ниже.
              backgroundColor: active
                ? 'var(--accent)'
                : (!muted && hoveredKey === opt.key)
                  ? 'color-mix(in srgb, var(--accent) 18%, transparent)'
                  : 'transparent',
              color: muted
                ? 'var(--text-muted)'
                : active
                  ? 'var(--text-inverse)'
                  : 'var(--text-primary)',
              opacity: muted ? 0.5 : 1,
              cursor: muted ? 'not-allowed' : 'pointer',
              whiteSpace: 'nowrap',
              transition: 'background-color 0.12s ease, color 0.12s ease',
            }}
          >
            {opt.label}
            {/* Замочек только для тарифного лока. У «нет данных» замка нет —
                иначе читается как «можно разблокировать». */}
            {opt.locked && !opt.disabled && <Lock size={11} className="flex-shrink-0" />}
          </button>
        );
      })}
    </div>
  );

  // Без trailing — возвращаем самостоятельную пилюлю как есть (десятки мест
  // используют переключатель именно так, рендер обязан остаться прежним).
  if (!trailing) return segmentGrid;

  // С trailing — внешняя flex-пилюля несёт рамку/фон/скругление и НЕ клиппит
  // (overflow видимый), чтобы поповер подсказки не обрезался. Внутри: сетка
  // сегментов слева и ячейка-подсказка с перегородкой справа.
  return (
    <div
      role="group"
      className={`frame-segmented rounded-full ${className}`}
      style={{
        backgroundColor: 'var(--bg-secondary)',
        border: '2px solid var(--text-primary)',
        display: 'inline-flex',
        alignItems: 'stretch',
      }}
    >
      {segmentGrid}
      <div
        style={{
          display: 'inline-flex',
          alignItems: 'center',
          borderLeft: '2px solid var(--text-primary)',
          padding: '0 var(--sp-2)',
        }}
      >
        {trailing}
      </div>
    </div>
  );
}
