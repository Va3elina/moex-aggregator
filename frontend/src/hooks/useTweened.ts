import { useEffect, useRef, useState } from 'react';

/** Пользователь попросил систему убрать анимации — значения меняются сразу. */
export function prefersReducedMotion(): boolean {
  return typeof window !== 'undefined' && !!window.matchMedia?.('(prefers-reduced-motion: reduce)').matches;
}

/** Кубический ease-out: быстро стартует, мягко садится на значение. */
export const easeOut = (k: number): number => 1 - Math.pow(1 - k, 3);

/**
 * useTweened — число, которое при смене значения за `ms` досчитывается до
 * нового, а не прыгает. Первый показ сразу с конечным значением; смена на
 * середине анимации продолжает с текущего промежуточного числа.
 */
export function useTweened(value: number, ms = 450): number {
  const [shown, setShown] = useState(value);
  const shownRef = useRef(value);
  useEffect(() => {
    const from = shownRef.current;
    if (from === value || prefersReducedMotion()) {
      shownRef.current = value;
      setShown(value);
      return;
    }
    const t0 = performance.now();
    let raf = 0;
    const frame = (now: number) => {
      const k = Math.min(1, (now - t0) / ms);
      const v = from + (value - from) * easeOut(k);
      shownRef.current = v;
      setShown(v);
      if (k < 1) raf = requestAnimationFrame(frame);
    };
    raf = requestAnimationFrame(frame);
    return () => cancelAnimationFrame(raf);
  }, [value, ms]);
  return shown;
}
