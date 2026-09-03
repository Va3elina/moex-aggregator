/**
 * MobileTopBar — компактный header для мобильной версии.
 *
 * Default mode (главные страницы индикаторов):
 *   - Логотип «Фрейм» (link на /)
 *   - Иконка темы / PLUS / Аватар справа
 *
 * Back mode (передан `onBack` через MobileLayout):
 *   - Логотип заменяется на ← back-button — natural cue для detail/
 *     secondary страниц (Profile, Pricing, Methodology)
 *   - Остальные кнопки справа те же
 */
import { Sun, Moon, ChevronLeft } from 'lucide-react';
import { Link } from 'react-router-dom';
import { useAuth } from '../../contexts/AuthContext';
import { useTheme } from '../../contexts/ThemeContext';
import FrameLogo from '../FrameLogo';
import MobileAnomalyBell from '../anomaly/MobileAnomalyBell';

interface MobileTopBarProps {
  /** Если задан — на месте логотипа рендерится ← back-кнопка. */
  onBack?: () => void;
}

export default function MobileTopBar({ onBack }: MobileTopBarProps) {
  const { user } = useAuth();
  const { theme, cycleTheme } = useTheme();
  const isLight = theme === 'editorial-light';

  // Первая буква имени для аватара (D, А, и т.д.)
  const avatar = user?.email?.[0]?.toUpperCase() ?? 'Г';

  return (
    <div className="fm-topbar">
      <div className="fm-topbar-left">
        {onBack ? (
          <button
            onClick={onBack}
            className="fm-back-btn"
            aria-label="Назад"
          >
            <ChevronLeft size={18} strokeWidth={2.4} />
            <span>Назад</span>
          </button>
        ) : (
          <>
            <Link to="/" style={{ textDecoration: 'none', display: 'inline-flex', alignItems: 'center' }}>
              <FrameLogo size={18} color="var(--accent)" />
            </Link>
          </>
        )}
      </div>
      <div className="fm-topbar-right">
        <MobileAnomalyBell />
        <button
          className="fm-icon-btn"
          aria-label="Сменить тему"
          onClick={cycleTheme}
        >
          {isLight ? <Moon size={16} strokeWidth={2.2} /> : <Sun size={16} strokeWidth={2.2} />}
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
