/**
 * EditorialChip — pill-кнопка с editorial дизайном (2px black border, 999px radius,
 * hard shadow при active). В non-editorial темах рендерится как обычная кнопка
 * с темо-зависимым фоном.
 *
 * Использование:
 *   <EditorialChip active={period === '1м'} onClick={() => setPeriod('1м')}>1М</EditorialChip>
 *   <EditorialChip active={mode === 'long'} accentVar="--oi-green">Покупки</EditorialChip>
 *
 * Когда нужен «уникальный» цвет (как в OI variants — амбер/зелёный/красный):
 *   accentVar="--oi-green"  → используется var(--oi-green) для заливки в active.
 *   По умолчанию — var(--accent).
 */
import type { ReactNode, CSSProperties } from 'react';
import { useTheme } from '../../contexts/ThemeContext';

interface Props {
  active: boolean;
  onClick?: () => void;
  disabled?: boolean;
  /** CSS-переменная для активного цвета (default: --accent) */
  accentVar?: string;
  /** Дополнительные классы — обычно не нужны, но иногда (mobile sizing) полезны */
  className?: string;
  /** Тонкий вариант (1.5px border, меньше padding) — для плотных групп вроде OI variants */
  size?: 'md' | 'sm';
  title?: string;
  children: ReactNode;
}

export default function EditorialChip({
  active, onClick, disabled, accentVar = '--accent', className = '', size = 'md', title, children,
}: Props) {
  const { theme } = useTheme();
  const isEditorial = theme.startsWith('editorial');

  const padding = size === 'sm' ? 'px-3 py-1.5' : 'px-4 py-2';
  const fontSize = size === 'sm' ? 'text-xs md:text-sm' : 'text-sm';

  const editorialActiveStyle: CSSProperties = {
    backgroundColor: `var(${accentVar})`,
    color: 'var(--text-inverse)',
    border: '2px solid var(--text-primary)',
    borderRadius: 999,
    boxShadow: 'var(--shadow-hard-chip)',
    fontWeight: 700,
  };
  const editorialInactiveStyle: CSSProperties = {
    backgroundColor: 'var(--bg-secondary)',
    color: 'var(--text-primary)',
    border: '2px solid var(--text-primary)',
    borderRadius: 999,
    fontWeight: 700,
  };

  // Non-editorial: обычные классы; active использует --accent через btn-control
  const nonEditorialClass = active
    ? 'btn-control active'
    : 'bg-theme-secondary text-theme-secondary hover:text-theme-primary border border-theme rounded-lg';

  return (
    <button
      onClick={onClick}
      disabled={disabled}
      title={title}
      className={`${padding} ${fontSize} font-medium transition-colors duration-200 ${
        disabled ? 'opacity-50 cursor-not-allowed' : ''
      } ${isEditorial ? '' : nonEditorialClass} ${className}`}
      style={isEditorial ? (active ? editorialActiveStyle : editorialInactiveStyle) : undefined}
    >
      {children}
    </button>
  );
}
