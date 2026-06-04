import { useEffect, useState } from 'react';

/**
 * usePersistedState — как useState, но начальное значение читается из localStorage
 * по ключу `key`, а любое изменение пишется обратно. Нужен, чтобы пользовательские
 * настройки индикаторов (период / режим / таймфрейм / группировка — «кнопки»)
 * не сбрасывались на новой сессии.
 *
 * Ключи неймспейсим как `frame:<индикатор>:<настройка>` (напр. `frame:oi:period`),
 * чтобы разные страницы не пересекались.
 *
 * Безопасность: JSON.parse в try/catch (битый/чужой формат → fallback на initial),
 * запись в try/catch (quota / приватный режим Safari → молча игнорируем).
 * SSR-safe (проверка typeof window).
 *
 * NB: восстановленное значение может оказаться недоступным по текущему тарифу
 * (tier-gating) — это нормально: страница сама ре-проверит доступ к восстановленной
 * настройке, как при ручном выборе.
 */
export function usePersistedState<T>(
  key: string,
  initial: T,
): [T, React.Dispatch<React.SetStateAction<T>>] {
  const [value, setValue] = useState<T>(() => {
    if (typeof window === 'undefined') return initial;
    try {
      const raw = window.localStorage.getItem(key);
      return raw != null ? (JSON.parse(raw) as T) : initial;
    } catch {
      return initial;
    }
  });

  useEffect(() => {
    if (typeof window === 'undefined') return;
    try {
      window.localStorage.setItem(key, JSON.stringify(value));
    } catch {
      /* localStorage недоступен (quota / private mode) — игнорируем */
    }
  }, [key, value]);

  return [value, setValue];
}
