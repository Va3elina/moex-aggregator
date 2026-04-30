/**
 * EditorialFrame — wrapper для виджета/секции страницы.
 *
 * В editorial темах: 2px black outline + hard shadow + 12px radius (бумажная карточка).
 * В остальных темах: обычный border-theme + soft shadow (как `.widget` класс).
 *
 * Используется чтобы оборачивать графики, group of controls, KPI-блоки и другие
 * структурные единицы страницы. Снижает количество inline-стилей в страницах.
 */
import type { ReactNode, CSSProperties } from 'react';
import { useTheme } from '../../contexts/ThemeContext';

interface Props {
  /** Размер shadow: 'hard' (default) или 'soft' (без editorial-выпадающей тени) */
  shadow?: 'hard' | 'soft' | 'none';
  /** Отступы внутри frame (default: 'md' = p-5) */
  padding?: 'none' | 'sm' | 'md' | 'lg';
  className?: string;
  style?: CSSProperties;
  children: ReactNode;
}

export default function EditorialFrame({
  shadow = 'hard', padding = 'md', className = '', style, children,
}: Props) {
  const { theme } = useTheme();
  const isEditorial = theme.startsWith('editorial');

  const padClass = {
    none: '',
    sm: 'p-3',
    md: 'p-5',
    lg: 'p-6 md:p-8',
  }[padding];

  const editorialStyle: CSSProperties = {
    border: '2px solid var(--text-primary)',
    borderRadius: 12,
    backgroundColor: 'var(--bg-secondary)',
    boxShadow: shadow === 'none' ? 'none'
      : shadow === 'soft' ? 'var(--shadow-md)'
      : 'var(--shadow-hard)',
  };
  const nonEditorialStyle: CSSProperties = {
    border: '1px solid var(--border-color)',
    borderRadius: 16,
    backgroundColor: 'var(--bg-secondary)',
    boxShadow: shadow === 'none' ? 'none' : 'var(--shadow-md)',
  };

  return (
    <div
      className={`${padClass} ${className}`}
      style={{ ...(isEditorial ? editorialStyle : nonEditorialStyle), ...style }}
    >
      {children}
    </div>
  );
}
