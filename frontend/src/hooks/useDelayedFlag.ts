import { useEffect, useState } from 'react';

/**
 * useDelayedFlag — true, только если `on` продержался дольше `ms`.
 *
 * Для индикаторов загрузки: быстрый ответ (из кэша, за сотню миллисекунд)
 * проходит без вспышки затемнения, а долгий честно показывает, что идёт
 * загрузка.
 */
export function useDelayedFlag(on: boolean, ms = 250): boolean {
  const [shown, setShown] = useState(false);
  useEffect(() => {
    if (!on) {
      setShown(false);
      return;
    }
    const id = window.setTimeout(() => setShown(true), ms);
    return () => window.clearTimeout(id);
  }, [on, ms]);
  return shown;
}
