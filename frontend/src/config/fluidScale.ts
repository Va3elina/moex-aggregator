/**
 * Fluid Design Scale — JS counterpart to CSS vars in index.css.
 *
 * ── Когда что использовать ──
 * • CSS-context (className/style font-size/padding/gap):
 *     style={{ fontSize: 'var(--fs-sm)' }}
 *   Браузер сам резолвит clamp() — fluid работает нативно.
 *
 * • SVG <text fontSize={...}> или любое JS-число:
 *     const fs = fluid.fsSm(vw)
 *   Возвращает число (в px) — точная копия CSS clamp.
 *
 * ── Правило ──
 * НИКАКИХ inline clamp() в компонентах. Используем эти токены.
 * Если нужен новый размер — добавляем сюда И в index.css :root.
 *
 * ── Расчёт ──
 * Зеркалит CSS clamp(MIN, Avw + Brem, MAX), где:
 *   1rem = 16px (root font), 1vw = window.innerWidth / 100.
 */

const cl = (min: number, val: number, max: number): number =>
  Math.max(min, Math.min(max, val));

const REM = 16; // root font-size

/** Type scale — px values matching :root --fs-* CSS vars */
export const fluid = {
  // ── Type ──
  fs2xs:  (vw: number) => cl(8,  0.4 * vw / 100 + 0.4 * REM,  11),
  fsXs:   (vw: number) => cl(10, 0.5 * vw / 100 + 0.45 * REM, 12.5),
  fsSm:   (vw: number) => cl(11, 0.5 * vw / 100 + 0.55 * REM, 14),
  fsBase: (vw: number) => cl(13, 0.55 * vw / 100 + 0.65 * REM, 16),
  fsLg:   (vw: number) => cl(14, 0.6 * vw / 100 + 0.75 * REM, 18),
  fsXl:   (vw: number) => cl(18, 1.0 * vw / 100 + 0.9 * REM,  22),
  fs2xl:  (vw: number) => cl(20, 1.5 * vw / 100 + 1.0 * REM,  28),
  fs3xl:  (vw: number) => cl(28, 2.5 * vw / 100 + 1.2 * REM,  56),

  // ── Spacing ──
  sp1: (vw: number) => cl(2,  0.4 * vw / 100, 4),
  sp2: (vw: number) => cl(4,  0.8 * vw / 100, 8),
  sp3: (vw: number) => cl(6,  1.2 * vw / 100, 12),
  sp4: (vw: number) => cl(10, 1.6 * vw / 100, 16),
  sp5: (vw: number) => cl(14, 2.0 * vw / 100, 20),
  sp6: (vw: number) => cl(18, 2.5 * vw / 100, 24),

  // ── Icons ──
  icoXs: (vw: number) => cl(12, 1.2 * vw / 100 + 0.4 * REM, 18),
  icoSm: (vw: number) => cl(14, 1.4 * vw / 100 + 0.5 * REM, 22),
  icoMd: (vw: number) => cl(18, 1.6 * vw / 100 + 0.6 * REM, 26),
  icoLg: (vw: number) => cl(24, 2.0 * vw / 100 + 0.8 * REM, 36),
} as const;

/**
 * React hook — ширина viewport с listener'ом resize.
 * Используй в компонентах которые рендерят SVG <text> или нуждаются
 * в JS-значениях fluid scale.
 */
import { useEffect, useState } from 'react';

export function useViewportWidth(): number {
  const [vw, setVw] = useState<number>(
    typeof window !== 'undefined' ? window.innerWidth : 1920
  );
  useEffect(() => {
    const onResize = () => setVw(window.innerWidth);
    window.addEventListener('resize', onResize);
    return () => window.removeEventListener('resize', onResize);
  }, []);
  return vw;
}
