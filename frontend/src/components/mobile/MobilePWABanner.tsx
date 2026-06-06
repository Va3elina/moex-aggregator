/**
 * MobilePWABanner — bottom sheet с CTA на установку PWA.
 *
 * Логика отображения:
 *   - На Android/Desktop Chrome: после `beforeinstallprompt` event +
 *     5 сек паузы (даём юзеру осмотреться) показываем banner
 *   - На iOS Safari: показываем инструкцию «Поделиться → На экран Домой»
 *   - Один раз dismissed → больше не показывается (localStorage flag)
 *   - Уже installed → не показывается никогда
 *
 * UI: bottom sheet прямо над BottomRail, не блокирует основной контент
 * (можно скролить chart за ним). Закрывается крестиком или backdrop.
 */
import { useEffect, useState } from 'react';
import { X, Smartphone, Share, Plus } from 'lucide-react';
import FrameLogo from '../FrameLogo';
import { usePWAInstall } from '../../hooks/usePWAInstall';
import { useAnalytics } from '../../contexts/AnalyticsContext';

const DISMISS_STORAGE_KEY = 'frame_pwa_install_dismissed_v1';
// Промо install'а должно вылезать ПОСЛЕДНИМ — после cookie-баннера и онбординг-
// тура. На первом визите всё валилось кучей и перекрывало друг друга. Поэтому
// (а) ждём пока юзер закроет cookie-consent (consent ≠ null), и только тогда
// (б) запускаем таймер; задержка достаточно велика, чтобы не наложиться на тур.
const SHOW_DELAY_MS = 12000;

export default function MobilePWABanner() {
  const { canInstall, canInstallIOS, promptInstall } = usePWAInstall();
  const { consent } = useAnalytics();
  const [visible, setVisible] = useState(false);
  const [dismissed, setDismissed] = useState<boolean>(() => {
    if (typeof window === 'undefined') return true;
    try {
      return localStorage.getItem(DISMISS_STORAGE_KEY) === '1';
    } catch {
      return true;
    }
  });

  // Auto-show после паузы, если можно установить. Не стартуем пока cookie-
  // consent не дан/отклонён (consent === null → баннер ещё висит) — иначе
  // install-промо наложится на cookie + тур. Эффект реагирует на consent.
  useEffect(() => {
    if (dismissed || (!canInstall && !canInstallIOS)) return;
    if (consent === null) return;
    const t = setTimeout(() => setVisible(true), SHOW_DELAY_MS);
    return () => clearTimeout(t);
  }, [canInstall, canInstallIOS, dismissed, consent]);

  function markDismissed() {
    setVisible(false);
    setDismissed(true);
    try {
      localStorage.setItem(DISMISS_STORAGE_KEY, '1');
    } catch {
      // ignore
    }
  }

  async function handleInstall() {
    const outcome = await promptInstall();
    setVisible(false);
    if (outcome === 'dismissed') markDismissed();
  }

  if (!visible) return null;

  return (
    <>
      <div className="fm-pwa-backdrop" onClick={() => setVisible(false)} aria-hidden />
      <div
        className="fm-pwa-banner"
        role="dialog"
        aria-labelledby="fm-pwa-title"
      >
        <button
          onClick={markDismissed}
          className="fm-pwa-close"
          aria-label="Закрыть"
        >
          <X size={16} strokeWidth={2.4} />
        </button>

        <div className="fm-pwa-header">
          <div className="fm-pwa-logo">
            <FrameLogo size={26} color="var(--accent)" />
          </div>
          <div>
            <h3 id="fm-pwa-title" className="fm-pwa-title">
              Установить Фрейм
            </h3>
            <p className="fm-pwa-subtitle">
              Быстрее, без адресной строки. Один тап с домашнего экрана.
            </p>
          </div>
        </div>

        {canInstall ? (
          <div className="fm-pwa-actions">
            <button onClick={handleInstall} className="fm-pwa-btn primary">
              <Smartphone size={16} strokeWidth={2.4} />
              <span>Установить</span>
            </button>
            <button onClick={markDismissed} className="fm-pwa-btn">
              Не сейчас
            </button>
          </div>
        ) : canInstallIOS ? (
          <div className="fm-pwa-ios-steps">
            <div className="fm-pwa-step">
              <span className="fm-pwa-step-num">1</span>
              <Share size={16} strokeWidth={2.4} style={{ color: 'var(--accent)' }} />
              <span>Тап по кнопке «Поделиться» в Safari</span>
            </div>
            <div className="fm-pwa-step">
              <span className="fm-pwa-step-num">2</span>
              <Plus size={16} strokeWidth={2.4} style={{ color: 'var(--accent)' }} />
              <span>Выбери «На экран Домой»</span>
            </div>
            <button onClick={markDismissed} className="fm-pwa-btn" style={{ marginTop: 12 }}>
              Понятно
            </button>
          </div>
        ) : null}
      </div>
    </>
  );
}
