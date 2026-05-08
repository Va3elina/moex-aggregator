/**
 * CookieConsentBanner — bottom popup для согласия на сбор аналитики.
 *
 * Показывается ОДИН раз пока user не нажмёт «Принять» или «Только необходимые».
 * После этого localStorage `frame_consent_v1` хранит выбор → banner не появляется.
 *
 * Логика:
 *  - 'accepted'  → AnalyticsProvider шлёт events
 *  - 'minimal'   → events НЕ шлются (только session-cookie auth остаётся)
 *  - null        → banner visible (consent ещё не дан)
 *
 * Дизайн — editorial-press chip-button style (paper bg + hard 1.5px border).
 * Размещение — bottom-fixed, не overlay (не блокирует контент сверху).
 */
import { Link } from 'react-router-dom';
import { Cookie } from 'lucide-react';
import { useAnalytics } from '../contexts/AnalyticsContext';

export default function CookieConsentBanner() {
  const { consent, setConsent } = useAnalytics();

  // Если пользователь уже выбрал — не показываем
  if (consent !== null) return null;

  return (
    <div
      role="dialog"
      aria-label="Согласие на сбор данных"
      className="fixed bottom-0 left-0 right-0 z-[100] border-t"
      style={{
        backgroundColor: 'var(--bg-primary)',
        borderColor: 'var(--text-primary)',
        borderTopWidth: '1.5px',
        boxShadow: '0 -4px 16px rgba(0,0,0,0.12)',
      }}
    >
      <div className="max-w-7xl mx-auto px-4 md:px-6 py-3 md:py-4 flex flex-col md:flex-row items-start md:items-center" style={{ gap: 'var(--sp-3)' }}>
        {/* Icon + text */}
        <div className="flex items-start flex-1" style={{ gap: 'var(--sp-3)' }}>
          <Cookie
            size={20}
            className="flex-shrink-0 mt-0.5"
            style={{ color: 'var(--accent)' }}
          />
          <p
            className="leading-snug"
            style={{
              color: 'var(--text-primary)',
              fontSize: 'var(--fs-sm)',
              maxWidth: 720,
            }}
          >
            Мы собираем обезличенную статистику посещений (страницы, события,
            время на сайте) — это помогает улучшать сервис. IP-адрес не
            сохраняется, контент-события привязаны к аккаунту только если вы
            залогинены.{' '}
            <Link
              to="/privacy"
              className="underline"
              style={{ color: 'var(--accent)' }}
            >
              Подробнее
            </Link>
          </p>
        </div>

        {/* Buttons */}
        <div className="flex items-center w-full md:w-auto" style={{ gap: 'var(--sp-2)' }}>
          <button
            onClick={() => setConsent('minimal')}
            className="editorial-press font-semibold rounded-full flex-1 md:flex-initial whitespace-nowrap"
            style={{
              backgroundColor: 'var(--bg-secondary)',
              color: 'var(--text-primary)',
              border: '1.5px solid var(--text-primary)',
              fontSize: 'var(--fs-sm)',
              padding: 'var(--sp-2) var(--sp-4)',
            }}
          >
            Только необходимые
          </button>
          <button
            onClick={() => setConsent('accepted')}
            className="editorial-press font-semibold rounded-full flex-1 md:flex-initial whitespace-nowrap"
            style={{
              backgroundColor: 'var(--accent)',
              color: '#fff',
              border: '1.5px solid var(--text-primary)',
              fontSize: 'var(--fs-sm)',
              padding: 'var(--sp-2) var(--sp-4)',
            }}
          >
            Принять
          </button>
        </div>
      </div>
    </div>
  );
}
