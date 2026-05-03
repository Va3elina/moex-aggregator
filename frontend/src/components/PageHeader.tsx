/**
 * PageHeader — единый header для всех индикаторных страниц.
 *
 * Заменяет inline-паттерн `<div className="flex items-center gap-3 mb-6">
 *   <div className="p-3 bg-gradient-to-br from-X to-Y rounded-xl"><Icon /></div>
 *   <div><h1/><p/></div></div>` который раньше дублировался на 9 страницах.
 *
 * Дизайн:
 *   - Иконка 40×40 с subtle accent-tint (color-mix) вместо кричащего gradient
 *   - Icon монохром в --accent цвете — сразу identifies индикатор, но без AI-look'а
 *   - Optional eyebrow (uppercase мелкий label сверху) — editorial pattern
 *
 * Usage:
 *   <PageHeader icon={CalendarDays} title="Сезонность" subtitle="..." />
 *   <PageHeader icon={Zap} eyebrow="ИНДИКАТОР" title="Сила рынка" />
 */
import type { LucideIcon } from 'lucide-react';
import HelpTooltip from './HelpTooltip';
import type { MethodologyEntry } from '../data/methodology';

interface PageHeaderProps {
  /** Иконка-идентификатор индикатора (Lucide) */
  icon?: LucideIcon;
  /** Маленький uppercase-лейбл над заголовком (editorial pattern) */
  eyebrow?: string;
  /** Основной заголовок */
  title: string;
  /** Подзаголовок под title */
  subtitle?: string;
  /** Доп. элементы справа (бейджи, кнопки, ссылки) */
  rightSlot?: React.ReactNode;
  /** Методология индикатора — показывает знак "?" рядом с заголовком. */
  help?: MethodologyEntry;
  /** Ссылка на отдельную страницу методологии. Если указана — "?" ведёт
   *  на эту страницу. Иначе — показывает popover с текстом help. */
  helpLink?: string;
}

export default function PageHeader({
  icon: Icon,
  eyebrow,
  title,
  subtitle,
  rightSlot,
  help,
  helpLink,
}: PageHeaderProps) {
  return (
    <div className="flex items-start mb-6" style={{ gap: 'var(--sp-3)' }}>
      {Icon && (
        <div className="page-header-icon flex items-center justify-center flex-shrink-0">
          <Icon size={22} strokeWidth={2} />
        </div>
      )}
      <div className="flex-1 min-w-0">
        {eyebrow && (
          <p
            className="uppercase mb-0.5"
            style={{
              color: 'var(--text-muted)',
              letterSpacing: '0.1em',
              fontWeight: 600,
              fontSize: 'var(--fs-2xs)',
            }}
          >
            {eyebrow}
          </p>
        )}
        <h1
          className="page-header-title font-bold"
          style={{
            color: 'var(--text-primary)',
            letterSpacing: '-0.01em',
            lineHeight: 1.2,
            display: 'flex',
            alignItems: 'center',
            gap: 4,
            flexWrap: 'wrap',
            fontSize: 'var(--fs-3xl)',
          }}
        >
          <span>{title}</span>
          {(help || helpLink) && <HelpTooltip entry={help} linkTo={helpLink} />}
        </h1>
        {subtitle && (
          <p
            className="mt-1"
            style={{ color: 'var(--text-secondary)', fontSize: 'var(--fs-sm)' }}
          >
            {subtitle}
          </p>
        )}
      </div>
      {rightSlot && <div className="flex-shrink-0">{rightSlot}</div>}
    </div>
  );
}
