import { useEffect, useState } from 'react';

/**
 * useGrowReveal — «grow-from-zero» волна прогресса для entrance-анимаций графиков.
 *
 * Возвращает массив progress[] длиной count, каждый ∈ [0, 1], со stagger
 * слева-направо: элемент i стартует с задержкой ∝ i. Easing — easeOutQuart.
 * Применение: высоту / ширину / opacity / scale элемента i умножай на
 * (progress[i] ?? 1) — fallback к 1 (полная величина), если массив короче.
 *
 * Перезапуск — при смене triggerKey (новые данные / фильтр / выбор). triggerKey
 * ОБЯЗАН меняться и при смене КОЛИЧЕСТВА элементов (включай длину/сигнатуру данных),
 * иначе при удлинении ряда правые элементы кадр висят на полной величине.
 *
 * Анти-флэш: синхронный сброс прогресса в рендере при смене ключа (React-паттерн
 * «adjust state during render») — иначе один кадр новые элементы видны целиком до
 * того, как до них дойдёт волна. Извлечено из StackedBidirectionalHistogram (CbrFlows).
 *
 * Уважает prefers-reduced-motion: при включённом — сразу все 1 (без анимации).
 */
export interface GrowRevealOptions {
  /** Шаг задержки между соседними элементами, мс (default 70). */
  stagger?: number;
  /** Длительность роста одного элемента, мс (default 900). */
  duration?: number;
  /** Потолок суммарного stagger, мс (default 800) — длинные ряды не растягиваются бесконечно. */
  maxStagger?: number;
}

const prefersReducedMotion = (): boolean =>
  typeof window !== 'undefined' &&
  typeof window.matchMedia === 'function' &&
  window.matchMedia('(prefers-reduced-motion: reduce)').matches;

export function useGrowReveal(
  count: number,
  triggerKey: string,
  opts: GrowRevealOptions = {},
): number[] {
  const { stagger = 70, duration = 900, maxStagger = 800 } = opts;
  const reduced = prefersReducedMotion();

  const [progress, setProgress] = useState<number[]>(
    () => new Array(Math.max(0, count)).fill(reduced ? 1 : 0),
  );

  // Анти-флэш: синхронный сброс при смене ключа (см. docstring).
  const [animatedKey, setAnimatedKey] = useState(triggerKey);
  if (triggerKey !== animatedKey) {
    setAnimatedKey(triggerKey);
    setProgress(new Array(Math.max(0, count)).fill(reduced ? 1 : 0));
  }

  useEffect(() => {
    if (count <= 0) return;
    if (reduced) {
      setProgress(new Array(count).fill(1));
      return;
    }
    const totalStagger = Math.min(maxStagger, count * stagger);
    const totalDuration = totalStagger + duration;
    const start = performance.now();
    let raf = 0;
    const tick = (now: number) => {
      const elapsed = now - start;
      if (elapsed >= totalDuration) {
        setProgress(new Array(count).fill(1));
        return;
      }
      setProgress(
        Array.from({ length: count }, (_, i) => {
          const delay = count > 1 ? (i / (count - 1)) * totalStagger : 0;
          const t = Math.min(1, Math.max(0, elapsed - delay) / duration);
          return 1 - Math.pow(1 - t, 4); // easeOutQuart
        }),
      );
      raf = requestAnimationFrame(tick);
    };
    raf = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [triggerKey, count, reduced]);

  return progress;
}
