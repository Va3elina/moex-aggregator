/**
 * MobileTopBar — компактный header для мобильной версии.
 *
 * Структура слева направо:
 *   - Логотип «Фрейм» (accent color) + Beta-тег
 *   - Иконка темы (theme toggle)
 *   - PLUS-кнопка (apricot/accent — ссылка на pricing)
 *   - Аватар (буква имени)
 */
import { Sun, Moon } from 'lucide-react';
import { Link } from 'react-router-dom';
import { useAuth } from '../../contexts/AuthContext';
import { useTheme } from '../../contexts/ThemeContext';

export default function MobileTopBar() {
  const { user } = useAuth();
  const { theme, cycleTheme } = useTheme();
  const isLight = theme === 'editorial-light';

  // Первая буква имени для аватара (D, А, и т.д.)
  const avatar = user?.email?.[0]?.toUpperCase() ?? 'Г';

  return (
    <div className="fm-topbar">
      <div className="fm-topbar-left">
        <Link to="/" className="fm-logo" style={{ textDecoration: 'none' }}>
          Фрейм
        </Link>
        <span className="fm-beta-tag">Beta</span>
      </div>
      <div className="fm-topbar-right">
        <button
          className="fm-icon-btn"
          aria-label="Сменить тему"
          onClick={cycleTheme}
        >
          {isLight ? <Moon size={14} /> : <Sun size={14} />}
        </button>
        <Link to="/pricing" className="fm-icon-btn fm-plus" style={{ textDecoration: 'none' }}>
          PLUS
        </Link>
        <Link to="/profile" className="fm-icon-btn fm-avatar" style={{ textDecoration: 'none' }}>
          {avatar}
        </Link>
      </div>
    </div>
  );
}
