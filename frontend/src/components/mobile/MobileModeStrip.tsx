/**
 * Полоска режимов индикатора + строка настроек — общий верх мобильных страниц,
 * у которых есть «подвиды»: открытые позиции (график ⇄ скринер), деньги в
 * фондах (категории), поток капитала (акции / ОФЗ / валюта), сделки фондов,
 * индикатор Баффетта, сезонность. Утверждено по макету «Полоска режимов на
 * всех индикаторах» (холст Claude Design, 2026-11).
 *
 * Заменяет на этих страницах MobilePageHeader: какой это индикатор, видно по
 * подсвеченному пункту нижнего меню, а место наверху отдано самим режимам —
 * раньше они прятались в sheet «Опции» и их не находили.
 *
 *   ┌──────────────┬──────────────┐   ← вкладки делят ширину поровну,
 *   │▔▔▔▔▔▔▔▔▔▔▔▔▔▔│              │     оранжевая черта СВЕРХУ активной
 *   │  Режим один  │  Режим два   │
 *   └──────────────┴──────────────┘
 *       Сбербанк · Чистая позиция · 1 год (?)   ← строка настроек по центру
 *
 * В полноэкранном режиме обе строки прячутся (mobile.css), как раньше заголовок.
 */
import { Lock } from 'lucide-react';
import { Link } from 'react-router-dom';

export interface ModeItem<K extends string> {
  key: K;
  label: string;
  /** Режим закрыт тарифом — рисуем замок, клик уходит в onLockedClick. */
  locked?: boolean;
  /** Режим недоступен вовсе (раздел «скоро»). */
  disabled?: boolean;
  title?: string;
}

interface StripProps<K extends string> {
  items: ModeItem<K>[];
  value: K;
  onChange: (key: K) => void;
  onLockedClick?: (key: K) => void;
  ariaLabel: string;
  tourId?: string;
}

export function MobileModeStrip<K extends string>({
  items, value, onChange, onLockedClick, ariaLabel, tourId,
}: StripProps<K>) {
  // 4–5 режимов не помещаются крупным шрифтом — мельче, длинная подпись
  // переносится на вторую строку внутри своей доли.
  const many = items.length > 3;
  return (
    <div className={`fm-modes${many ? ' many' : ''}`} role="tablist" aria-label={ariaLabel} data-tour={tourId}>
      {items.map((it) => {
        const active = it.key === value;
        return (
          <button
            key={it.key}
            type="button"
            role="tab"
            aria-selected={active}
            aria-disabled={it.disabled || it.locked || undefined}
            title={it.title}
            className={`fm-mode${active ? ' active' : ''}`}
            onClick={() => {
              if (it.disabled) return;
              if (it.locked) { onLockedClick?.(it.key); return; }
              if (!active) onChange(it.key);
            }}
          >
            <span>{it.label}</span>
            {it.locked && <Lock size={11} strokeWidth={2.4} aria-hidden />}
          </button>
        );
      })}
    </div>
  );
}

interface ContextProps {
  /** Текущие настройки одной строкой: актив, вариант, период. */
  text: string;
  /** Методология — кружок «?» в той же строке, сразу за текстом. */
  helpLink?: string;
  /** Источник данных — мелкая строка ниже (атрибуция, где она обязательна). */
  note?: string;
}

export function MobileContextLine({ text, helpLink, note }: ContextProps) {
  return (
    <div className="fm-context">
      <div className="fm-context-row">
        <span className="fm-context-text">{text}</span>
        {helpLink && (
          <Link to={helpLink} className="fm-context-help" aria-label="Методология">?</Link>
        )}
      </div>
      {note && <div className="fm-context-note">{note}</div>}
    </div>
  );
}
