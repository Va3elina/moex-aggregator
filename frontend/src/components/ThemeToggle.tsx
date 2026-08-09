import { useTheme } from '../contexts/ThemeContext';
import { useAnalytics } from '../contexts/AnalyticsContext';
import ThemeGlyph from './ThemeGlyph';

export default function ThemeToggle() {
  const { theme, setTheme } = useTheme();
  const { track } = useAnalytics();
  const isDark = theme === 'editorial-dark';

  const toggle = () => {
    const nextTheme = isDark ? 'editorial-light' : 'editorial-dark';
    setTheme(nextTheme);
    track('theme_toggle', { to: nextTheme });
  };

  // Размер унифицирован с другими header-icons: 40 → 36 → 32 на mobile/md/xl.
  // SVG-иконки масштабируются пропорционально (14-18-20 через clamp).
  return (
    <button
      onClick={toggle}
      aria-label={isDark ? 'Светлая тема' : 'Тёмная тема'}
      title={isDark ? 'Светлая тема' : 'Тёмная тема'}
      className="editorial-press relative grid place-items-center overflow-hidden"
      style={{
        color: 'var(--text-primary)',
        border: '1.5px solid var(--text-primary)',
        borderRadius: 999,
        backgroundColor: 'transparent',
        width: 'clamp(22px, 1.6vw + 0.3rem, 32px)',
        height: 'clamp(22px, 1.6vw + 0.3rem, 32px)',
      }}
    >
      {/* Пара Sun/Moon с кроссфейдом — общий компонент (та же анимация в песочнице). */}
      <ThemeGlyph dark={isDark} size="clamp(13px, 1vw + 0.3rem, 17px)" />
    </button>
  );
}
