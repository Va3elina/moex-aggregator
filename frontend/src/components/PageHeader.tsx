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
}

export default function PageHeader({
  icon: Icon,
  eyebrow,
  title,
  subtitle,
  rightSlot,
}: PageHeaderProps) {
  return (
    <div className="flex items-start gap-3 mb-6">
      {Icon && (
        <div
          className="flex items-center justify-center flex-shrink-0"
          style={{
            width: 40,
            height: 40,
            borderRadius: 'var(--radius-md, 8px)',
            // subtle accent-tinted background (12% opacity of accent)
            background: 'color-mix(in srgb, var(--accent) 12%, transparent)',
            color: 'var(--accent)',
          }}
        >
          <Icon size={20} strokeWidth={1.8} />
        </div>
      )}
      <div className="flex-1 min-w-0">
        {eyebrow && (
          <p
            className="text-[10px] uppercase mb-0.5"
            style={{
              color: 'var(--text-muted)',
              letterSpacing: '0.1em',
              fontWeight: 600,
            }}
          >
            {eyebrow}
          </p>
        )}
        <h1
          className="text-2xl font-bold"
          style={{
            color: 'var(--text-primary)',
            letterSpacing: '-0.01em',
            lineHeight: 1.2,
          }}
        >
          {title}
        </h1>
        {subtitle && (
          <p
            className="text-sm mt-1"
            style={{ color: 'var(--text-secondary)' }}
          >
            {subtitle}
          </p>
        )}
      </div>
      {rightSlot && <div className="flex-shrink-0">{rightSlot}</div>}
    </div>
  );
}
