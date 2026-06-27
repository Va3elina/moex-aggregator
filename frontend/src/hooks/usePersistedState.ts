import { useCallback, useEffect, useRef, useState } from 'react';

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
 *
 * `seed` (опц.) — внешнее начальное значение приоритетнее localStorage: задаётся,
 * когда состояние должно прийти из URL (напр. диплинк Telegram-сигнала на /oi).
 * Применяется СИНХРОННО на init — чтобы первый рендер/запрос шёл уже с нужным
 * значением, а не после правки эффектом. undefined → обычная localStorage-логика.
 */
export function usePersistedState<T>(
  key: string,
  initial: T,
  seed?: T,
): [T, React.Dispatch<React.SetStateAction<T>>] {
  const [value, setValue] = useState<T>(() => {
    if (seed !== undefined) return seed;
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

/**
 * usePersistedSet — версия usePersistedState для Set'ов. Обычный usePersistedState
 * не годится: JSON.stringify(new Set([1,2])) === '{}', данные теряются. Здесь Set
 * сериализуем как массив (Array.from) и читаем обратно через new Set(arr).
 *
 * Ключ можно неймспейсить по текущей подвыборке, напр.
 * `frame:funds:hidden:${category}`: при смене ключа набор ПЕРЕЧИТЫВАЕТСЯ под новый
 * ключ. Это заменяет прежний «сброс в пустой Set при смене категории» и заодно
 * запоминает выбор для каждой категории отдельно. Запись идёт синхронно в сеттере
 * под актуальный ключ (keyRef), а не отдельным эффектом — иначе смена ключа
 * успела бы записать старый набор в новый ключ и затереть его.
 *
 * Контракт безопасности тот же, что у usePersistedState: JSON.parse/запись в
 * try/catch (битый формат / quota / приватный режим → fallback на initial), SSR-safe.
 */
export function usePersistedSet<T>(
  key: string,
  initial?: Set<T>,
): [Set<T>, React.Dispatch<React.SetStateAction<Set<T>>>] {
  const readSet = (k: string): Set<T> => {
    const fallback = initial ?? new Set<T>();
    if (typeof window === 'undefined') return fallback;
    try {
      const raw = window.localStorage.getItem(k);
      if (raw == null) return fallback;
      const parsed = JSON.parse(raw);
      return Array.isArray(parsed) ? new Set(parsed as T[]) : fallback;
    } catch {
      return fallback;
    }
  };

  const keyRef = useRef(key);
  const [value, setValueRaw] = useState<Set<T>>(() => readSet(key));

  // Смена ключа (неймспейс по категории/типу) → перечитываем набор под новый
  // ключ. На маунте keyRef.current === key, поэтому пропускаем: state уже готов.
  useEffect(() => {
    if (keyRef.current === key) return;
    keyRef.current = key;
    setValueRaw(readSet(key));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key]);

  const setValue = useCallback<React.Dispatch<React.SetStateAction<Set<T>>>>((action) => {
    setValueRaw((prev) => {
      const next =
        typeof action === 'function'
          ? (action as (p: Set<T>) => Set<T>)(prev)
          : action;
      if (typeof window !== 'undefined') {
        try {
          window.localStorage.setItem(keyRef.current, JSON.stringify(Array.from(next)));
        } catch {
          /* quota / private mode — игнорируем */
        }
      }
      return next;
    });
  }, []);

  return [value, setValue];
}
