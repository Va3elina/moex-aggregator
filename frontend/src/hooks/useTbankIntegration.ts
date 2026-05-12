/**
 * useTbankIntegration — загрузка T-Bank JS SDK (Integration.js) +
 * инициализация PaymentIntegration.
 *
 * Идея:
 *  - Скрипт `https://integrationjs.t-static.ru/integration.js` загружаем один
 *    раз на всю сессию (singleton через module-scope promise).
 *  - При первом использовании создаём <script async> и ждём load.
 *  - После load'а вызываем PaymentIntegration.init({terminalKey, ...}) и
 *    возвращаем готовый integration объект.
 *  - Если terminalKey не передан → hook ничего не делает (полезно когда
 *    провайдер не tbank).
 *
 * Использование:
 *
 *   const { integration, ready, error } = useTbankIntegration(terminalKey)
 *   useEffect(() => {
 *     if (!ready) return
 *     const widget = integration.payments.create({ widgetTypes: ['sbp', 'tpay'] })
 *     widget.setPaymentStartCallback(async () => ({
 *       paymentUrl: await fetchPaymentUrl(),
 *     }))
 *     widget.mount('#paymentContainer')
 *   }, [ready])
 */
import { useEffect, useState } from 'react';
import type {
  TBankIntegration,
  TBankIntegrationInitConfig,
} from '../types/tbank';

const SDK_SRC = 'https://integrationjs.t-static.ru/integration.js';

// Singleton: загружаем скрипт один раз за сессию.
let scriptPromise: Promise<void> | null = null;

function loadScript(): Promise<void> {
  if (scriptPromise) return scriptPromise;
  scriptPromise = new Promise((resolve, reject) => {
    // Если скрипт уже добавлен (например, из SSR / preload) — не добавляем снова
    const existing = document.querySelector<HTMLScriptElement>(
      `script[src="${SDK_SRC}"]`
    );
    if (existing) {
      // Если уже загружен — резолвим сразу
      if (window.PaymentIntegration) {
        resolve();
        return;
      }
      // Если ещё грузится — слушаем load
      existing.addEventListener('load', () => resolve());
      existing.addEventListener('error', () =>
        reject(new Error('T-Bank SDK failed to load'))
      );
      return;
    }
    const s = document.createElement('script');
    s.src = SDK_SRC;
    s.async = true;
    s.onload = () => resolve();
    s.onerror = () => reject(new Error('T-Bank SDK failed to load'));
    document.body.appendChild(s);
  });
  return scriptPromise;
}

// Singleton per terminalKey: init() не дёргаем повторно
const initCache = new Map<string, Promise<TBankIntegration>>();

function getOrInit(terminalKey: string): Promise<TBankIntegration> {
  const cached = initCache.get(terminalKey);
  if (cached) return cached;
  const p = (async () => {
    await loadScript();
    if (!window.PaymentIntegration) {
      throw new Error('window.PaymentIntegration missing after script load');
    }
    const config: TBankIntegrationInitConfig = {
      terminalKey,
      product: 'eacq',
      features: { payment: {} },
    };
    return window.PaymentIntegration.init(config);
  })();
  initCache.set(terminalKey, p);
  return p;
}

export interface UseTbankIntegrationResult {
  integration: TBankIntegration | null;
  ready: boolean;
  error: Error | null;
}

export function useTbankIntegration(
  terminalKey: string | null | undefined
): UseTbankIntegrationResult {
  const [integration, setIntegration] = useState<TBankIntegration | null>(null);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    if (!terminalKey) return;
    let cancelled = false;
    getOrInit(terminalKey)
      .then((i) => {
        if (cancelled) return;
        setIntegration(i);
      })
      .catch((e: unknown) => {
        if (cancelled) return;
        setError(e instanceof Error ? e : new Error(String(e)));
      });
    return () => {
      cancelled = true;
    };
  }, [terminalKey]);

  return { integration, ready: !!integration, error };
}
