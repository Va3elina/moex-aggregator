/**
 * usePWAInstall — обработка PWA install prompt.
 *
 * Принцип:
 *   - Browser fires `beforeinstallprompt` когда сайт installable (Android
 *     Chrome, Edge). Мы перехватываем event и preventDefault — браузер
 *     не показывает свой mini-bar. Дальше юзер триггерит install через
 *     наш кастомный banner.
 *   - iOS Safari НЕ поддерживает beforeinstallprompt. Для них fallback
 *     instruction "Поделиться → На экран Домой".
 *   - Если приложение уже installed (display-mode: standalone) — все
 *     prompts скрываются.
 *
 * Detection:
 *   - canInstall — Chrome/Edge поймали `beforeinstallprompt`
 *   - canInstallIOS — iOS Safari (есть navigator.standalone), не in
 *     standalone сейчас
 *   - isInstalled — уже в standalone (PWA mode)
 */
import { useCallback, useEffect, useState } from 'react';

interface BeforeInstallPromptEvent extends Event {
  prompt(): Promise<void>;
  userChoice: Promise<{ outcome: 'accepted' | 'dismissed' }>;
}

function detectStandalone(): boolean {
  if (typeof window === 'undefined') return false;
  if (window.matchMedia('(display-mode: standalone)').matches) return true;
  // iOS Safari устанавливает navigator.standalone
  const nav = window.navigator as { standalone?: boolean };
  return nav.standalone === true;
}

function detectIOSSafari(): boolean {
  if (typeof navigator === 'undefined') return false;
  const ua = navigator.userAgent;
  const isIOS = /iPhone|iPad|iPod/.test(ua);
  if (!isIOS) return false;
  // Chrome на iOS тоже WebKit, но не показывает Add to Home Screen
  const isChrome = /CriOS|FxiOS|EdgiOS|OPiOS/.test(ua);
  return !isChrome;
}

export function usePWAInstall() {
  const [deferredPrompt, setDeferredPrompt] =
    useState<BeforeInstallPromptEvent | null>(null);
  const [isInstalled, setIsInstalled] = useState<boolean>(() =>
    detectStandalone(),
  );
  const [isIOSSafari] = useState<boolean>(() => detectIOSSafari());

  useEffect(() => {
    function onBeforeInstall(e: Event) {
      e.preventDefault();
      setDeferredPrompt(e as BeforeInstallPromptEvent);
    }

    function onAppInstalled() {
      setIsInstalled(true);
      setDeferredPrompt(null);
    }

    window.addEventListener('beforeinstallprompt', onBeforeInstall);
    window.addEventListener('appinstalled', onAppInstalled);
    return () => {
      window.removeEventListener('beforeinstallprompt', onBeforeInstall);
      window.removeEventListener('appinstalled', onAppInstalled);
    };
  }, []);

  const promptInstall = useCallback(async () => {
    if (!deferredPrompt) return null;
    try {
      await deferredPrompt.prompt();
      const choice = await deferredPrompt.userChoice;
      setDeferredPrompt(null);
      return choice.outcome;
    } catch (err) {
      console.error('PWA install error:', err);
      return null;
    }
  }, [deferredPrompt]);

  return {
    canInstall: !!deferredPrompt && !isInstalled,
    canInstallIOS: isIOSSafari && !isInstalled,
    isInstalled,
    promptInstall,
  };
}
