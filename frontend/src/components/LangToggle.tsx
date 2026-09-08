import { useTranslation } from 'react-i18next';
import { getLang, setLang } from '../i18n';
import { useAnalytics } from '../contexts/AnalyticsContext';

/**
 * Переключатель языка RU/EN. Тот же размер и outline-стиль, что у ThemeToggle,
 * чтобы стоять с ним в одном ряду в шапке. Показывает язык, НА который
 * переключит (как ThemeToggle показывает противоположную тему).
 */
export default function LangToggle({ className = '' }: { className?: string }) {
  // Подписка на смену языка — чтобы кнопка перерисовалась.
  useTranslation();
  const { track } = useAnalytics();
  const lang = getLang();
  const next = lang === 'ru' ? 'en' : 'ru';

  const toggle = () => {
    setLang(next);
    track('lang_toggle', { to: next });
  };

  return (
    <button
      onClick={toggle}
      aria-label={next === 'en' ? 'Switch to English' : 'Переключить на русский'}
      title={next === 'en' ? 'English' : 'Русский'}
      className={`editorial-press grid place-items-center rounded-full ${className}`}
      style={{
        color: 'var(--text-primary)',
        border: '1.5px solid var(--text-primary)',
        backgroundColor: 'transparent',
        width: 'clamp(22px, 1.6vw + 0.3rem, 32px)',
        height: 'clamp(22px, 1.6vw + 0.3rem, 32px)',
        fontSize: 'clamp(9px, 0.55vw + 0.25rem, 11px)',
        fontWeight: 700,
        letterSpacing: '0.04em',
        lineHeight: 1,
        fontFamily: 'var(--font-mono, inherit)',
      }}
    >
      {next.toUpperCase()}
    </button>
  );
}
