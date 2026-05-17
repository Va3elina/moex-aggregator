/**
 * MobilePageHeader — заголовок страницы индикатора.
 *
 * Структура: иконка (40-44px квадрат с accent fill + hard shadow) +
 * title (с «?» иконкой методологии) + subtitle.
 *
 * Соответствует desktop'ному PageHeader, но компактнее и в editorial-стиле
 * с rounded square иконкой.
 */
import { Link } from 'react-router-dom';
import type { LucideIcon } from 'lucide-react';

interface MobilePageHeaderProps {
  Icon: LucideIcon;
  title: string;
  subtitle?: string;
  helpLink?: string;
}

export default function MobilePageHeader({
  Icon,
  title,
  subtitle,
  helpLink,
}: MobilePageHeaderProps) {
  return (
    <div className="fm-pageheader">
      <div className="fm-pageheader-icon">
        <Icon strokeWidth={2.4} />
      </div>
      <div style={{ flex: 1, minWidth: 0 }}>
        <h1 className="fm-pageheader-title">
          <span>{title}</span>
          {helpLink && (
            <Link
              to={helpLink}
              className="fm-help"
              style={{ textDecoration: 'none' }}
              aria-label="Методология"
            >
              ?
            </Link>
          )}
        </h1>
        {subtitle && <p className="fm-pageheader-subtitle">{subtitle}</p>}
      </div>
    </div>
  );
}
