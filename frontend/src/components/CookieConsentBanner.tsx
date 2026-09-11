/**
 * CookieConsentBanner — bottom popup про использование cookies.
 *
 * Формат уведомления (как у крупных РФ-магазинов): один текст + одна кнопка
 * «Окей». Показывается ОДИН раз, после клика localStorage `frame_consent_v1`
 * хранит выбор → banner не появляется.
 *
 * Логика:
 *  - 'accepted'  → AnalyticsProvider шлёт events (ставится кликом «Окей»)
 *  - 'minimal'   → events НЕ шлются; из баннера больше не выставляется,
 *                  но значение остаётся валидным для старых юзеров и для
 *                  тумблера отказа в профиле
 *  - null        → banner visible (юзер ещё не кликал); с 2026-09-11 события
 *                  шлются и в этом состоянии — баннер только уведомляет
 *
 * Дизайн — editorial-press chip-button style (paper bg + hard 1.5px border).
 * Размещение — bottom-fixed, не overlay (не блокирует контент сверху).
 */
import { Link } from 'react-router-dom';
import { Cookie } from 'lucide-react';
import { useAnalytics } from '../contexts/AnalyticsContext';
import { useTranslation } from 'react-i18next';

export default function CookieConsentBanner() {
  const { consent, setConsent } = useAnalytics();
  const { t } = useTranslation();

  // Если пользователь уже выбрал — не показываем
  if (consent !== null) return null;

  return (
    <div
      role="dialog"
      aria-label={t('Уведомление об использовании cookies')}
      className="fixed bottom-0 left-0 right-0 z-[100] border-t"
      style={{
        backgroundColor: 'var(--bg-primary)',
        borderColor: 'var(--text-primary)',
        borderTopWidth: '1.5px',
        boxShadow: '0 -4px 16px rgba(0,0,0,0.12)',
        paddingBottom: 'env(safe-area-inset-bottom, 0px)',
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
            {t('Мы используем')}{' '}
            <Link
              to="/privacy"
              className="underline"
              style={{ color: 'var(--accent)' }}
            >
              cookies
            </Link>
            {t(', чтобы анализировать, как вы пользуетесь сайтом, и делать сервис удобнее. IP-адрес не сохраняется.')}
          </p>
        </div>

        {/* Button */}
        <div className="flex items-center w-full md:w-auto">
          <button
            onClick={() => setConsent('accepted')}
            className="editorial-press font-semibold rounded-full flex-1 md:flex-initial whitespace-nowrap"
            style={{
              backgroundColor: 'var(--accent)',
              color: '#fff',
              border: '1.5px solid var(--text-primary)',
              fontSize: 'var(--fs-sm)',
              padding: 'var(--sp-2) var(--sp-6)',
            }}
          >
            {t('Окей')}
          </button>
        </div>
      </div>
    </div>
  );
}
