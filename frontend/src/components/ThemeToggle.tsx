import { useTheme } from '../contexts/ThemeContext';
import { useAnalytics } from '../contexts/AnalyticsContext';

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
      {/* Sun — видна в light. Лучи разлетаются + центр пульсирует. */}
      <svg
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.75"
        strokeLinecap="round"
        strokeLinejoin="round"
        className="absolute transition-all duration-500 ease-out"
        style={{
          width: 'clamp(13px, 1vw + 0.3rem, 17px)',
          height: 'clamp(13px, 1vw + 0.3rem, 17px)',
          opacity: isDark ? 0 : 1,
          transform: isDark ? 'rotate(-90deg) scale(0.4)' : 'rotate(0) scale(1)',
        }}
      >
        <circle cx="12" cy="12" r="4" fill="var(--accent)" stroke="var(--accent)" />
        <path d="M12 2v2" />
        <path d="M12 20v2" />
        <path d="M4.93 4.93l1.41 1.41" />
        <path d="M17.66 17.66l1.41 1.41" />
        <path d="M2 12h2" />
        <path d="M20 12h2" />
        <path d="M4.93 19.07l1.41-1.41" />
        <path d="M17.66 6.34l1.41-1.41" />
      </svg>

      {/* Moon — видна в dark. Acccent-fill, плавно влетает. */}
      <svg
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.75"
        strokeLinecap="round"
        strokeLinejoin="round"
        className="absolute transition-all duration-500 ease-out"
        style={{
          width: 'clamp(13px, 1vw + 0.3rem, 17px)',
          height: 'clamp(13px, 1vw + 0.3rem, 17px)',
          opacity: isDark ? 1 : 0,
          transform: isDark ? 'rotate(0) scale(1)' : 'rotate(90deg) scale(0.4)',
        }}
      >
        <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" fill="var(--accent)" stroke="var(--accent)" />
      </svg>
    </button>
  );
}
