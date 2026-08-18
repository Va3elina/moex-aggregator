import { useEffect, useRef, useState } from 'react';
import { ANIMATION } from '../../config/chartTheme';

/**
 * Entrance-reveal только серий графика (оси/сетка/легенда видны сразу):
 * rAF-анимация ширины rect внутри <clipPath> от 0 до fullWidth.
 *
 * Декларативные варианты НЕ работают (выяснено в SimpleChart, #1123/#1125/#1156):
 * содержимое <clipPath> не рендерится и CSS-анимации к нему не применяются,
 * а clip-path: inset() на вложенной SVG <g> Chromium тоже не анимирует.
 * Поэтому ширина анимируется JS'ом — тот же механизм, что в SimpleChart.
 *
 * ⚠️ Нарочно БЕЗ prefers-reduced-motion: на Windows «эффекты анимации» у многих
 * выключены системно, и такой гард молча убивал reveal у реальных юзеров (#1157).
 *
 * @param start     запуск анимации (обычно флаг «первый рендер с данными»);
 *                  пока false — возвращается 0 (серии скрыты, флеша полной
 *                  картинки перед стартом нет).
 * @param fullWidth текущая полная ширина области серий (px или viewBox-юниты);
 *                  читается через ref — resize не перезапускает анимацию.
 * @param restartKey смена значения перезапускает reveal (например animTrigger
 *                  при смене актива).
 * @returns ширина clip-rect; null = анимация закончена — брать fullWidth,
 *          чтобы rect следовал за resize'ом: `width={revealW ?? fullWidth}`.
 */
export function useChartReveal(
  start: boolean,
  fullWidth: number,
  restartKey?: unknown,
): number | null {
  const [revealW, setRevealW] = useState<number | null>(0);
  const widthRef = useRef(fullWidth);
  widthRef.current = fullWidth;
  const rafRef = useRef<number | null>(null);
  // Reveal играет ОДИН раз за жизнь компонента (схема как в OI). Без doneRef
  // мигание start false→true (смена актива: два фетча, данные на кадр
  // «рассинхронены» и hasData фликает) переигрывало reveal — линия заново
  // ползла слева, хотя кругляши/бары уже стояли. Явный restartKey
  // по-прежнему перезапускает.
  const doneRef = useRef(false);
  const keyRef = useRef(restartKey);

  useEffect(() => {
    if (!start) return;
    const keyChanged = keyRef.current !== restartKey;
    keyRef.current = restartKey;
    if (doneRef.current && !keyChanged) return;
    const D = ANIMATION.revealDuration;
    let t0: number | null = null;
    const step = (ts: number) => {
      if (t0 == null) t0 = ts;
      const t = Math.min((ts - t0) / D, 1);
      setRevealW(ANIMATION.easing(t) * widthRef.current);
      if (t < 1) {
        rafRef.current = requestAnimationFrame(step);
      } else {
        doneRef.current = true;
        setRevealW(null);
      }
    };
    setRevealW(0);
    rafRef.current = requestAnimationFrame(step);
    return () => {
      if (rafRef.current) cancelAnimationFrame(rafRef.current);
    };
  }, [start, restartKey]);

  return revealW;
}
